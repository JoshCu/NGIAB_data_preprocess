from flask import Blueprint, render_template, request, jsonify
import geopandas as gpd
import requests

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
