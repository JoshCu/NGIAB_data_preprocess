// Function to handle map click events
// globals that should be a cookie or something
// this is why I don't call myself a full stack developer
var wb_id_dict = {};
var selected_wb_layer = null;
var upstream_maps = {};

async function update_selected() {
    console.log('updating selected');
    if (!(Object.keys(wb_id_dict).length === 0)) {



        fetch('/get_geojson_from_wbids', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(wb_id_dict),
        })
            .then(response => response.json())
            .then(data => {
                // if the wb_id is already in the dict, remove the key
                // remove the old layer
                if (selected_wb_layer) {
                    map.removeLayer(selected_wb_layer);
                }
                console.log(data);
                // add the new layer
                selected_wb_layer = L.geoJSON(data, {
                    onEachFeature: colorlayer,
                    style: {
                        color: "#eb34d8",
                        opacity: 0.6,
                        fillColor: '#e0abff',
                        fillOpacity: 0
                    }
                }).addTo(map);
            }).then()
            .catch(error => {
                console.error('Error:', error);
            });
    } else {
        if (selected_wb_layer) {
            map.removeLayer(selected_wb_layer);
        }
    }
    await populate_upstream();
    document.getElementById('selected-basins').textContent = Object.keys(wb_id_dict).join(', ');
}

async function populate_upstream() {
    console.log('populating upstream selected');
    // drop any key that is not in the wb_id_dict
    for (const [key, value] of Object.entries(upstream_maps)) {
        if (!(key in wb_id_dict)) {
            map.removeLayer(value);
            delete upstream_maps[key];
        }
    }
    // add any key that is in the wb_id_dict but not in the upstream_maps
    for (const [key, value] of Object.entries(wb_id_dict)) {
        if (!(key in upstream_maps)) {
            upstream_maps[key] = null;
        }
    }
    if (Object.keys(upstream_maps).length === 0) {
        return;
    }

    const fetchPromises = Object.entries(upstream_maps).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_upstream_geojson_from_wbids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ [key]: value }),
            })
                .then(response => response.json())
                .then(data => {
                    // if the wb_id is already in the dict, remove the key
                    // remove the old layer
                    if (upstream_maps[key]) {
                        map.removeLayer(upstream_maps[key]);
                    }
                    console.log(data);
                    // add the new layer
                    upstream_maps[key] = L.geoJSON(data).addTo(map);
                })
                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });

    await Promise.all(fetchPromises);
    if (selected_wb_layer) {
        selected_wb_layer.bringToFront();
    }
}

function colorlayer(feature, layer) {
    layer.on('mouseover', function (e) {
        layer.setStyle({
            fillOpacity: 0.4
        });
    });
    layer.on('mouseout', function (e) {
        layer.setStyle({
            fillOpacity: 0.1
        });
    });
}


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
            // if the wb_id is already in the dict, remove the key
            if (data['wb_id'] in wb_id_dict) {
                delete wb_id_dict[data['wb_id']];
            }
            else {
                wb_id_dict[data['wb_id']] = [lat, lng];
            }
            console.log('clicked on wb_id: ' + data['wb_id'] + ' coords :' + lat + ', ' + lng);
            update_selected();
        })
        .catch(error => {
            console.error('Error:', error);
        });


}

async function get_boundary(vpu_code) {
    // calculate the bounds from the grid bounds
    const proxyUrl = '/get_map_data';
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

async function addLayers() {
    await Promise.all(Object.keys(geometry_urls).map(async (key) => {
        var geometryUrl = 'HS-' + geometry_urls[key] + ':' + key + '_boundaries@EPSG:900913';
        bounds = await get_boundary(geometryUrl);
        var layer = L.tileLayer(baseUrl + geometryUrl + '@png/{z}/{x}/{-y}.png', {
            attribution: '&copy; <a href="https://nationalmap.gov/">National Map</a> contributors',
            transparent: true,
            format: 'image/png',
            opacity: 0.5,
            minZoom: 8,
            maxZoom: 18,
            reuseTiles: true,
            bounds: bounds,
        }).addTo(map);
        layer.on("tileloadstart",profile_loader.start_loading);
        layer.on("tileload",profile_loader.stop_loading);
        layer.on("tileabort",profile_loader.abort_load);
    }));
    map.on('click', onMapClick);
}

async function subset() {
    console.log('subsetting');
    fetch('/subset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(wb_id_dict),
    })
        .then(response => response.text())
        .then(filename => {
            console.log(filename);
            // popup with the file name
            document.getElementById('output-path').textContent = "subset to " + filename;
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

async function forcings() {
    console.log('getting forcings');
    const forcing_dir = document.getElementById('output-path').textContent;
    const start_time = document.getElementById('start-time').value;
    const end_time = document.getElementById('end-time').value;
    if (forcing_dir === '' || start_time === '' || end_time === '') {
        alert('Please enter a valid output path, start time, and end time');
        document.getElementById('time-warning').style.color = 'red';
        return;
    }
    fetch('/forcings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time }),
    })
        .catch(error => {
            console.error('Error:', error);
        });
}

geometry_urls = {
    '16': 'e8ddee6a8a90484fa7a976458e79c0c3',
    '01': '5f0e81c665314967a1e15e4ae672aaae',
    '02': '131a6d6cc6514b558f968716783d7d47',
    '03N': '38c84132987243c2a49ffb9d178f3162',
    '03S': '5d9cdd0b6851460aaccd0c83557e4a6c',
    '03W': '5674050a194c41b8a61f000c94c27983',
    '04': 'd161033e07634d6199ae136a24807f22',
    '05': '47113551c63b41daa53465aee6cb69e9',
    '06': '1302f07176cd46e2ab70db730e601682',
    '07': 'b380393bebaf47e68afd98fb15f4ff10',
    '08': '2391aadf1f4440499e7b61b4dcc41d94',
    '09': '27670ef43fbf42be914e1fca7d41ce0b',
    '10L': 'b5028b1c8b5240f8b7deb3bcebc2f005',
    '10U': 'b6dca803df5a4a8c8120512ccdfe8ba9',
    '11': '8e7a4c951c8241269e47ee461c1d9ef3',
    '12': '8ea1c9e098f044318777bf283c1fc0ad',
    '13': 'b166308dffed4db39083393a894c3694',
    '15': '68501dc3b6214aca8d92aaae75aee941',
    '16': '1244ac2f25b0442cacece320424c6756',
    '17': 'da20b06af50d4adab080597ae4ae8c46',
    '18': 'ca2e56965245476fbcb258b7d2aec7ab',
    '14': '2d78b60ad0cf469daced4c4aa37764ad',
}

// Initialize the map
var map = L.map('map').setView([42, -102], 4);

// Add OpenStreetMap tiles to the map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '© OpenStreetMap contributors',
    crs: L.CRS.EPSG3857
}).addTo(map);

//map.on('click', onMapClick);
var baseUrl = "https://geoserver.hydroshare.org/geoserver/gwc/service/tms/1.0.0/";  // Base URL of the WMTS service

var boundaries_of_vpus = "HS-35e8c6023c154b6298fcda280beda849:vpu_boundaries@EPSG:900913";

southWest = L.latLng(22.5470, -129.4137);
northEast = L.latLng(51.0159, -68.9337);
bounds = L.latLngBounds(southWest, northEast);

var wmtsLayer = L.tileLayer(baseUrl +
    boundaries_of_vpus + '@png/{z}/{x}/{-y}.png', {
    attribution: '&copy; <a href="https://nationalmap.gov/">National Map</a> contributors',
    transparent: true,
    format: 'image/png',
    opacity: 1,
    maxZoom: 7,
    bounds: bounds,
}).addTo(map);

addLayers();

// Register the click event listener for the map
// add listener for the #subset-button
document.getElementById('subset-button').addEventListener('click', subset);
// add listener for the #subset-button
document.getElementById('forcings-button').addEventListener('click', forcings);
