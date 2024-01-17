from flask import Blueprint, render_template, request, jsonify
import geopandas as gpd
import requests
import re
import math

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

def get_wbid_from_point(coords):
    # takes a point and returns the wbid of the watershed it is in
    hs_wms_vpu_all = f'HS-35e8c6023c154b6298fcda280beda849'

    url = f'https://geoserver.hydroshare.org/geoserver/{hs_wms_vpu_all}/wfs?' \
            'service=wfs&version=2.0.0&' \
            f'request=getFeature&' \
            'srsName=EPSG:4269&' \
            f'bbox={coords[1]},{coords[0]},{coords[1]},{coords[0]},EPSG:4269&' \
            f'typeName=vpu_boundaries&' \
            'outputFormat=json&' \
            'PropertyName=VPU'
    print(url)

    q = requests.Request('GET', url).prepare().url
    df = gpd.read_file(q, format='json')
    print(df)
    # exit if a VPU is not found, i.e. a user doesn't click on the layer
    if len(df) == 0: return
    
    VPU = df.VPU.values[0]
    hs_wms_res = f'HS-{geometry_urls[VPU]}'
    print(f'You selected VPU {VPU}')
    url = f'https://geoserver.hydroshare.org/geoserver/{hs_wms_res}/wfs?' \
            'service=wfs&version=2.0.0&' \
            f'request=getFeature&' \
            'srsName=EPSG:4326&' \
            f'bbox={coords[1]},{coords[0]},{coords[1]},{coords[0]},EPSG:4326&' \
            f'typeName={VPU}_boundaries&' \
            'outputFormat=json&'

    q = Request('GET', url).prepare().url
    df = gpd.read_file(q, format='json')



@main.route('/handle_map_interaction', methods=['POST'])
def handle_map_interaction():
    data = request.get_json()
    coordinates = data.get('coordinates')
    wb_id = get_wbid_from_point(coordinates)
    result = {
        'status': 'success',
        'message': 'Received coordinates: {}'.format(coordinates)
    }
    return jsonify(result)

def convert_grid_to_coords(xmin, ymin, xmax, ymax):
    # converts to lat/lon EPSG:4326
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