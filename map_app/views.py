from flask import Blueprint, render_template, request, jsonify
import geopandas as gpd
import requests
import re
import math
from pathlib import Path
from shapely.geometry import Point
import json
from subset import get_upstream_ids, get_graph

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

def get_wbid_from_point(coords):
    # inpute coords are EPSG:4326
    print(coords)
    # takes a point and returns the wbid of the watershed it is in
    # create a geometry mask for the point
    # load the watershed boundaries
    q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg" 
    d = {'col1': ['point'], 'geometry': [Point(coords['lng'], coords['lat'])]}
    point = gpd.GeoDataFrame(d, crs="EPSG:4326")
    df = gpd.read_file(q, format='GPKG', layer='divides', mask=point)
    return df['id'].values[0]



@main.route('/handle_map_interaction', methods=['POST'])
def handle_map_interaction():
    data = request.get_json()
    coordinates = data.get('coordinates')
    wb_id = get_wbid_from_point(coordinates)
    result = {
        'status': 'success',
        'message': 'Received coordinates: {}'.format(coordinates),
        'wb_id': wb_id
    }
    return jsonify(result)

def convert_grid_to_coords(xmin, ymin, xmax, ymax):
    # converts tile x,y index to lat/lon EPSG:4326
    zoom = 18
    n = 2 ** zoom
    xmin = xmin / n * 360.0 - 180.0
    xmax = xmax / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ymin / n)))
    ymin = lat_rad * 180.0 / math.pi
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ymax / n)))
    ymax = lat_rad * 180.0 / math.pi
    return [xmin, abs(ymin), xmax, abs(ymax)]

@main.route('/get_map_data', methods=['GET'])
def get_map_data():
    # Get the URL from the query string
    url = request.args.get('url')
    if not url:
        return jsonify({"error": "Missing URL parameter"}), 400
    try:
        # Make the request to the external URL
        response = requests.get(url)
        if response.status_code == 404:
            results = re.findall(r'[0-9]+(?=,)', response.content.decode('utf-8'))
            results = results[:4]
            results = [float(x) for x in results]
            bounds = convert_grid_to_coords(*results)
        return bounds, 200
    
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500
    
def wbids_to_geojson(wb_dict):
    for k, v in wb_dict.items():
        wb_dict[k] = Point(v[1], v[0])
    d = {'col1': wb_dict.keys(), 'geometry': wb_dict.values()}
    points = gpd.GeoDataFrame(d, crs="EPSG:4326")
    print(points)
    q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg" 
    df = gpd.read_file(q, format='GPKG', layer='divides', mask=points)
    # convert crs to 4326
    df = df.to_crs(epsg=4326)
    return df.to_json()

@main.route('/get_geojson_from_wbids', methods=['POST'])
def get_geojson_from_wbids():
    wb_dict = json.loads(request.data.decode('utf-8'))
    print(wb_dict)
    if len(wb_dict) == 0:
        return [], 204
    try:
        return wbids_to_geojson(wb_dict), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@main.route('/get_upstream_geojson_from_wbids', methods=['POST'])
def get_upstream_geojson_from_wbids():
    
    wb_dict = json.loads(request.data.decode('utf-8'))
    print(wb_dict)
    if len(wb_dict) == 0:
        return [], 204
    geopackage = Path(__file__).parent.parent / "data_sources" / "conus.gpkg"
    graph = get_graph(geopackage)
    # get upstream ids
    # get nexus points wb flows to
    # use nexus points as mask to load geojson
    try:
        return wbids_to_geojson(wb_dict), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500