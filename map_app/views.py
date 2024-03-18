import json
import math
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import requests
import shapely as sh
from flask import Blueprint, jsonify, render_template, request
from shapely import unary_union
from shapely.geometry import LineString, MultiLineString, Point
from shapely.wkb import loads

import data_processing.gpkg_utils as gpkg_u
from data_processing.create_realization import create_cfe_wrapper
from data_processing.file_paths import file_paths
from data_processing.forcings import create_forcings
from data_processing.graph_utils import get_flow_lines_in_set, get_upstream_ids, wbids_groupby_component
from data_processing.subset import subset

main = Blueprint("main", __name__)
intra_module_db = {}


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
    # if None in wb_dict.values():
    
    # for k, v in wb_dict.items():
        # wb_dict[k] = Point(v[1], v[0])
        # wb_dict[k] = Point(v.get("lng"), v.get("lat"))
    # d = {"col1": wb_dict.keys(), "geometry": wb_dict.values()}
    # points = gpd.GeoDataFrame(d, crs="EPSG:5070")
    # with open(file_paths.root_output_dir() / "full_gdf.json", "w") as f:
    #     f.write(points.to_json())
    # points = points.to_crs(crs="EPSG:5070")
    # print(points)
    # q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg"
    try:
        # print("reading file")
        # df = gpd.read_file(q, format="GPKG", layer="divides", mask=points)
        # print("read file")
        geom_df = gpkg_u.get_geom_from_wbids_map(wb_dict.keys())
        subsets = wbids_groupby_component(wb_dict.keys())
        wb_subset_indices = {}
        for i, subset in enumerate(subsets):
            for wb in subset:
                wb_subset_indices[wb] = i
        geom_df_filtered = {k: v for k, v in geom_df.items() if k in wb_subset_indices}
        d = {"col1": list(geom_df_filtered.keys()), "geometry": list(geom_df_filtered.values())}
        d["subset"] = [wb_subset_indices[k] for k in d["col1"]]
        gdf = gpd.GeoDataFrame(d, crs="EPSG:5070")
        gdf_simplify = gdf.dissolve(by="subset")
        gdf_simplify = gdf_simplify.to_crs(epsg=4326)
        return gdf_simplify.to_json()
    except Exception as e:
        print("error reading file")
        print(e)
        raise Exception(e.args).with_traceback(e.__traceback__)
    # convert crs to 4326
    df = df.to_crs(epsg=4326)
    return df.to_json()


@main.route("/get_geojson_from_wbids", methods=["POST"])
def get_geojson_from_wbids():
    wb_dict = json.loads(request.data.decode("utf-8"))
    print(len(wb_dict), "wbids")
    # wb_dict = gpkg_u.get_points_from_wbids(wb_dict)
    print(len(wb_dict), "points")
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
    line_dict = get_flow_lines_in_set(upstream_ids)
    # format ids as ('id1', 'id2', 'id3')
    geopackage = file_paths.conus_hydrofabric()
    sql_query = f"SELECT id, geom FROM divides WHERE id IN {tuple(upstream_ids)}"
    # remove the trailing comma from single element tuples
    sql_query = sql_query.replace(",)", ")")
    # get nexus locations
    nexi_keys = list(line_dict["to_wbs"].keys())
    sql_query2 = f"SELECT id, geom FROM nexus WHERE id IN {tuple(nexi_keys)}"
    sql_query2 = sql_query2.replace(",)", ")")
    # would be nice to use geopandas here but it doesn't support sql on geopackages
    con = sqlite3.connect(geopackage)
    result = con.execute(sql_query).fetchall()
    result2 = con.execute(sql_query2).fetchall()
    con.close()
    # convert the blobs to geometries
    geoms = {}
    nexs = {}
    geometry_list = []
    print(f"sql returned at {datetime.now()}")
    for r in result:
        geometry = blob_to_geometry(r[1])
        if geometry is not None:
            geometry_list.append(geometry)
            geoms[r[0]] = geometry.centroid
    for r in result2:
        geometry = blob_to_geometry(r[1])
        if geometry is not None:
            nexs[r[0]] = geometry.centroid
    print(f"converted blobs to geometries at {datetime.now()}")
    to_lines = []
    for wb, nex in line_dict["to_lines"]:
        if wb not in geoms or nex not in nexs:
            continue
        to_lines.append(LineString([geoms[wb], nexs[nex]]))
    lngth = sum(x.length for x in to_lines) / max(len(to_lines), 1)
    nexs_dir = []
    nex_pts = []
    for nex, targets in line_dict["to_wbs"].items():
        if not nex in nexs:
            continue
        for target in targets:
            if not target in geoms:
                continue
            nexs_dir.append(LineString([nexs[nex], geoms[target]]))
        nex_pts.append(nexs[nex].buffer(lngth / 4, 8))
    merged_tolines = unary_union(to_lines)
    merged_nexs = unary_union(nexs_dir)

    # split geometries into chunks and run unary_union in parallel
    merged_geometry = unary_union(geometry_list)
    # create a geodataframe from the geometry
    d1 = {"col1": [upstream_ids[0] + "_merged_geometry"], "geometry": [merged_geometry]}
    d2 = {"col1": [upstream_ids[0] + "_to_lines"], "geometry": [merged_tolines]}
    d3 = {"col1": [upstream_ids[0] + "_from_nexus"], "geometry": [merged_nexs]}
    d4 = {"col1": [], "geometry": []}
    for i, pt in enumerate(nex_pts):
        d4["col1"].append(upstream_ids[0] + "_nex_circles" + str(i))
        d4["geometry"].append(pt)
    ds = {
        "merged_geometry": d1,
        "merged_tolines": d2,
        "merged_from_nexus": d3,
        "nexus_circles": d4,
    }
    gs = {}
    for k, d in ds.items():
        gs[k] = gpd.GeoDataFrame(d, crs="EPSG:5070")
    return gs


@main.route("/get_upstream_geojson_from_wbids", methods=["POST"])
def get_upstream_geojson_from_wbids():
    print(f"getting graph at {datetime.now()}")
    wb_id = json.loads(request.data.decode("utf-8"))
    upstream_ids = get_upstream_ids(wb_id)
    print(f"got upstream ids at {datetime.now()}")
    upstream_ids = list(set(upstream_ids))
    gdfs = get_geodf_from_wb_ids(upstream_ids)
    print(f"got geodf at {datetime.now()}")
    for k in gdfs:
        gdfs[k] = gdfs[k].to_crs(epsg=4326)
    print(f"converted crs at {datetime.now()}")

    def serialize_geodf(obj):
        if isinstance(obj, gpd.GeoDataFrame):
            return obj.to_json()
        else:
            raise TypeError()

    return json.dumps(gdfs, default=serialize_geodf), 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    wb_ids = list(json.loads(request.data.decode("utf-8")).keys())
    print(wb_ids)
    subset_geopackage = subset(wb_ids)
    return subset_geopackage, 200


@main.route("/subset_to_file", methods=["POST"])
def subset_to_file():
    wb_ids = list(json.loads(request.data.decode("utf-8")).keys())
    print(wb_ids)
    total_subset = get_upstream_ids(wb_ids)
    total_subset = list(filter(lambda x: "wb" in x, total_subset))
    total_subset = sorted(total_subset)
    with open(file_paths.root_output_dir() / "subset.txt", "w") as f:
        f.write("\n".join(total_subset))
    return "success", 200


@main.route("/forcings", methods=["POST"])
def get_forcings():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    wb_id = data.get("forcing_dir").split("/")[-1]
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    # print(intra_module_db)
    app = intra_module_db["app"]
    debug = app.debug
    if debug:
        app.debug = False
        print(f"get_forcings() disabled debug mode at {datetime.now()}")
    try:
        create_forcings(start_time, end_time, wb_id)
    except Exception as e:
        if debug:
            app.debug = True
        print(f"get_forcings() failed with error: {str(e)}")
        raise e
    if debug:
        app.debug = True
        print(f"get_forcings() re-enabled debug mode at {datetime.now()}")
    return "success", 200


@main.route("/realization", methods=["POST"])
def get_realization():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    wb_id = data.get("forcing_dir").split("/")[-1]
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    create_cfe_wrapper(wb_id, start_time, end_time)
    return "success", 200


@main.route("/get_vpu", methods=["POST"])
def get_vpu():
    vpu_boundaries = gpkg_u.get_vpu_gdf()
    return vpu_boundaries.to_json(), 200


@main.route("/get_wbids_from_vpu", methods=["POST"])
def get_wbids_from_vpu():
    vpu = json.loads(request.data.decode("utf-8"))
    vpu = sh.geometry.shape(vpu)
    # convert to crs 5070
    vpu = gpd.GeoDataFrame({"geometry": [vpu]}, crs="EPSG:4326")
    vpu = vpu.to_crs(epsg=5070)
    wbs = gpd.read_file(file_paths.data_sources() / "conus.gpkg", layer="divides", mask=vpu)
    wbs = wbs.to_crs(epsg=4326)
    wbs = wbs[wbs["id"].notna()]
    # return dict[id: [lat, lon]]
    return (
        json.dumps(
            dict(zip(wbs["id"], zip(wbs["geometry"].centroid.x, wbs["geometry"].centroid.y)))
        ),
        200,
    )


@main.route("/logs", methods=["GET"])
def get_logs():
    log_file_path = "app.log"
    try:
        with open(log_file_path, "r") as file:
            # Read the last 100 lines
            lines = file.readlines()[-100:]
            lines = [line.strip() for line in lines]
            # reverse the lines so the most recent is first
            # easier than fixgin the frontend to auto scoll to the bottom
            reversed_lines = list(reversed(lines))
            # manual cast because jsonify is awful at type evaluation
            return jsonify({"logs": reversed_lines}), 200
    except Exception as e:
        return jsonify({"error": str(e)})
