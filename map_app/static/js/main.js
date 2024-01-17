// Function to handle map click events
function onMapClick(event) {
    // Extract the clicked coordinates
    var lat = event.latlng.lat;
    var lng = event.latlng.lng;

    // Send an AJAX request to the Flask backend
    fetch('/handle_map_interaction', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            coordinates: { lat: lat, lng: lng }
        }),
    })
    .then(response => response.json())
    .then(data => {
        // Process the response data
        // This will depend on what your Flask backend returns
        // For example, updating the map with new layers or markers
        console.log(data); // Log the data for debugging

        // Example: Add a marker at the clicked location
        L.marker([lat, lng]).addTo(map)
            .bindPopup('You clicked basin ' + data['wb_id'] + '\n coords :' + lat + ', ' + lng)
            .openPopup();
    })
    .catch(error => {
        console.error('Error:', error);
    });
}

// Initialize the map
var map = L.map('map').setView([41.74614949822607, -111.76617850993877], 2);

// Add OpenStreetMap tiles to the map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '© OpenStreetMap contributors',
    crs: L.CRS.EPSG3857
}).addTo(map);

var gagueslayer = L.esri.featureLayer({
    url: "https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer/0"
})

var waterbasinlayer = L.esri.featureLayer({
    url: "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/0"
})


function queryLayer() {
    // Query the ArcGIS layer for features within the current view
    var bounds = map.getBounds();
    gagueslayer.query()
        .within(bounds)
        .where("FCODE = 36700")
        .run(function(error, featureCollection) {
            if (error) {
                console.error('Query error:', error);
                return;
            }
            // Do something with the features, e.g., update a layer on the map
            //L.geoJSON(featureCollection).addTo(map);
        });
        waterbasinlayer.query()
        .within(bounds)
        .where("hudigit = 2")
        .run(function(error, featureCollection) {
            if (error) {
                console.error('Query error:', error);
                return;
            }
            // Do something with the features, e.g., update a layer on the map
            L.geoJSON(featureCollection).addTo(map);
    });

    // add the layer to the map
}

var baseUrl = "http://geoserver.hydroshare.org/geoserver/gwc/service/tms/1.0.0/";  // Base URL of the WMTS service

async function get_boundary(vpu_code){
    // calculate the bounds from the grid bounds
    const proxyUrl = 'http://localhost:5000/get_map_data';
    url = baseUrl + vpu_code + '@png/18/0/0.png';
    // tile url will 404 if the tile doesn't exist, and then return the bounds in the body
    //e.g. Coverage [minx,miny,maxx,maxy] is [53410, 154705, 65609, 162724, 18], index [x,y,z] is [0, 0, 18]

    try {
        const response = await fetch(`${proxyUrl}?url=${encodeURIComponent(url)}`);
        bounds = await response.json();
    } catch (error) {
        console.error('Error fetching boundary:', error);
    }
    // regex to find the bounds ([0-9]+,) match 0-4 are xmin, ymin, xmax, ymax
    // convert to lat long
    var xmin = bounds[0];
    var ymin = bounds[1];
    var xmax = bounds[2];
    var ymax = bounds[3];
    return L.latLngBounds(L.latLng(ymin, xmin), L.latLng(ymax, xmax));
}

var boundaries_of_vpus = "HS-35e8c6023c154b6298fcda280beda849:vpu_boundaries@EPSG:900913";

southWest = L.latLng(22.5470, -129.4137),
northEast = L.latLng(51.0159, -68.9337),
bounds = L.latLngBounds(southWest, northEast);

var wmtsLayer = L.tileLayer(baseUrl + 
    boundaries_of_vpus + '@png/{z}/{x}/{-y}.png', {
    attribution: '&copy; <a href="https://nationalmap.gov/">National Map</a> contributors',
    transparent: true,
    format: 'image/png',
    opacity: 0.8,
    maxZoom: 6,
    bounds: bounds,    
}).addTo(map);

geometry_urls = {
    '16':  'e8ddee6a8a90484fa7a976458e79c0c3',
    '01':  '5f0e81c665314967a1e15e4ae672aaae',
    '02':  '131a6d6cc6514b558f968716783d7d47',
    '03N': '38c84132987243c2a49ffb9d178f3162',
    '03S': '5d9cdd0b6851460aaccd0c83557e4a6c',
    '03W': '5674050a194c41b8a61f000c94c27983',
    '04':  'd161033e07634d6199ae136a24807f22',
    '05':  '47113551c63b41daa53465aee6cb69e9',
    '06':  '1302f07176cd46e2ab70db730e601682',
    '07':  'b380393bebaf47e68afd98fb15f4ff10',
    '08':  '2391aadf1f4440499e7b61b4dcc41d94',
    '09':  '27670ef43fbf42be914e1fca7d41ce0b',
    '10L': 'b5028b1c8b5240f8b7deb3bcebc2f005',
    '10U': 'b6dca803df5a4a8c8120512ccdfe8ba9',
    '11':  '8e7a4c951c8241269e47ee461c1d9ef3',
    '12':  '8ea1c9e098f044318777bf283c1fc0ad',
    '13':  'b166308dffed4db39083393a894c3694',
    '15':  '68501dc3b6214aca8d92aaae75aee941',
    '16':  '1244ac2f25b0442cacece320424c6756',
    '17':  'da20b06af50d4adab080597ae4ae8c46',
    '18':  'ca2e56965245476fbcb258b7d2aec7ab',
    '14':  '2d78b60ad0cf469daced4c4aa37764ad',
}

async function addLayers() {
    await Promise.all(Object.keys(geometry_urls).map(async (key) => {      
        var geometryUrl = 'HS-'+geometry_urls[key] + ':'+key+'_boundaries@EPSG:900913';
        bounds = await get_boundary(geometryUrl);
        L.tileLayer(baseUrl + geometryUrl + '@png/{z}/{x}/{-y}.png', {
            attribution: '&copy; <a href="https://nationalmap.gov/">National Map</a> contributors',
            transparent: true,
            format: 'image/png',
            opacity: 0.5,
            minZoom: 7,
            maxZoom: 16,
            reuseTiles: true,
            bounds: bounds,
        }).addTo(map);
    }));
}

addLayers();

// Register the click event listener for the map
map.on('click', onMapClick);
