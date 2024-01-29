import json
import math
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import requests
from flask import Blueprint, jsonify, render_template, request
from shapely import unary_union
from shapely.geometry import Point
from shapely.wkb import loads

from data_processing.file_paths import file_paths
from data_processing.graph_utils import get_upstream_ids
from data_processing.subset import subset
from data_processing.forcings import create_forcings


main = Blueprint("main", __name__)


@main.route("/")
def index():
    return render_template("index.html")


def get_wbid_from_point(coords):
    # inpute coords are EPSG:4326
    print(coords)
    # takes a point and returns the wbid of the watershed it is in
    # create a geometry mask for the point
    # load the watershed boundaries
    q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg"
    d = {"col1": ["point"], "geometry": [Point(coords["lng"], coords["lat"])]}
    point = gpd.GeoDataFrame(d, crs="EPSG:4326")
    df = gpd.read_file(q, format="GPKG", layer="divides", mask=point)
    return df["id"].values[0]


@main.route("/handle_map_interaction", methods=["POST"])
def handle_map_interaction():
    data = request.get_json()
    coordinates = data.get("coordinates")
    wb_id = get_wbid_from_point(coordinates)
    result = {
        "status": "success",
        "message": "Received coordinates: {}".format(coordinates),
        "wb_id": wb_id,
    }
    return jsonify(result)


def convert_grid_to_coords(xmin, ymin, xmax, ymax):
    # converts tile x,y index to lat/lon EPSG:4326
    zoom = 18
    n = 2**zoom
    xmin = xmin / n * 360.0 - 180.0
    xmax = xmax / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ymin / n)))
    ymin = lat_rad * 180.0 / math.pi
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ymax / n)))
    ymax = lat_rad * 180.0 / math.pi
    return [xmin, abs(ymin), xmax, abs(ymax)]


@main.route("/get_map_data", methods=["GET"])
def get_map_data():
    # Get the URL from the query string
    url = request.args.get("url")
    if not url:
        return jsonify({"error": "Missing URL parameter"}), 400
    try:
        # Make the request to the external URL
        response = requests.get(url)
        if response.status_code == 404:
            results = re.findall(r"[0-9]+(?=,)", response.content.decode("utf-8"))
            results = results[:4]
            results = [float(x) for x in results]
            bounds = convert_grid_to_coords(*results)
        return bounds, 200

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


def wbids_to_geojson(wb_dict):
    for k, v in wb_dict.items():
        wb_dict[k] = Point(v[1], v[0])
    d = {"col1": wb_dict.keys(), "geometry": wb_dict.values()}
    points = gpd.GeoDataFrame(d, crs="EPSG:4326")
    print(points)
    q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg"
    df = gpd.read_file(q, format="GPKG", layer="divides", mask=points)
    # convert crs to 4326
    df = df.to_crs(epsg=4326)
    return df.to_json()


@main.route("/get_geojson_from_wbids", methods=["POST"])
def get_geojson_from_wbids():
    wb_dict = json.loads(request.data.decode("utf-8"))
    print(wb_dict)
    if len(wb_dict) == 0:
        return [], 204
    try:
        return wbids_to_geojson(wb_dict), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def blob_to_geometry(blob):
    # from http://www.geopackage.org/spec/#gpb_format
    # byte 0-2 don't need
    # byte 3 bit 0 (bit 24)= 0 for little endian, 1 for big endian (used for srs id and envelope type)
    # byte 3 bit 1-3 (bit 25-27)= envelope type (needed to calculate envelope size)
    # byte 3 bit 4 (bit 28)= empty geometry flag
    envelope_type = (blob[3] & 14) >> 1
    empty = (blob[3] & 16) >> 4
    if empty:
        return None
    envelope_sizes = [0, 32, 48, 48, 64]
    envelope_size = envelope_sizes[envelope_type]
    header_byte_length = 8 + envelope_size
    # everything after the header is the geometry
    geom = blob[header_byte_length:]
    # convert to hex
    geometry = loads(geom)
    return geometry


def get_geodf_from_wb_ids(upstream_ids):
    # format ids as ('id1', 'id2', 'id3')
    geopackage = file_paths.conus_hydrofabric()
    sql_query = f"SELECT id, geom FROM divides WHERE id IN {tuple(upstream_ids)}"
    # remove the trailing comma from single element tuples
    sql_query = sql_query.replace(",)", ")")
    # would be nice to use geopandas here but it doesn't support sql on geopackages
    con = sqlite3.connect(geopackage)
    result = con.execute(sql_query).fetchall()
    con.close()
    # convert the blobs to geometries
    geometry_list = []
    print(f"sql returned at {datetime.now()}")
    for r in result:
        geometry = blob_to_geometry(r[1])
        if geometry is not None:
            geometry_list.append(geometry)
    print(f"converted blobs to geometries at {datetime.now()}")
    # split geometries into chunks and run unary_union in parallel
    merged_geometry = unary_union(geometry_list)
    # create a geodataframe from the geometry
    d = {"col1": [upstream_ids[0]], "geometry": [merged_geometry]}
    gdf = gpd.GeoDataFrame(d, crs="EPSG:5070")
    return gdf


@main.route("/get_upstream_geojson_from_wbids", methods=["POST"])
def get_upstream_geojson_from_wbids():
    print(f"getting graph at {datetime.now()}")
    wb_id = json.loads(request.data.decode("utf-8"))
    upstream_ids = get_upstream_ids(wb_id)
    print(f"got upstream ids at {datetime.now()}")
    upstream_ids = list(set(upstream_ids))
    gdf = get_geodf_from_wb_ids(upstream_ids)
    print(f"got geodf at {datetime.now()}")
    gdf = gdf.to_crs(epsg=4326)
    print(f"converted crs at {datetime.now()}")
    return gdf.to_json(), 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    wb_ids = list(json.loads(request.data.decode("utf-8")).keys())
    print(wb_ids)
    subset_geopackage = subset(wb_ids)
    return subset_geopackage, 200


@main.route("/forcings", methods=["POST"])
def get_forcings():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    forcing_dir = data.get("forcing_dir").split("/")[-1]
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    create_forcings(start_time, end_time, forcing_dir)
    return "success", 200
