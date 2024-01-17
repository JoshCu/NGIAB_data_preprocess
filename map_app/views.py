from flask import Blueprint, render_template, request, jsonify
import geopandas as gpd
import requests
import re
import math

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

@main.route('/handle_map_interaction', methods=['POST'])
def handle_map_interaction():
    data = request.get_json()
    coordinates = data.get('coordinates')

    # Here, you can add the logic to process the coordinates
    # For example, making a request to an external API, processing data, etc.
    # This will depend on your specific application's requirements

    # Example: Just echoing back the received coordinates
    result = {
        'status': 'success',
        'message': 'Received coordinates: {}'.format(coordinates)
    }

    # Return a JSON response
    return jsonify(result)

def convert_grid_to_coords(xmin, ymin, xmax, ymax):
    # accept a list of grid coordinates and return a list of lat/lon coordinates
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
            # text.match(/([0-9]+,){0}/)[0].slice(0,-1);
            results = re.findall(r'[0-9]+(?=,)', response.content.decode('utf-8'))
            results = results[:4]
            # cast to float
            results = [float(x) for x in results]
            bounds = convert_grid_to_coords(*results)
            return bounds, 200
        # Return the content of the response
        return response.content, response.status_code
    except requests.RequestException as e:
        # Handle any errors that occur during the request
        return jsonify({"error": str(e)}), 500