async function subset() {
    if (Object.keys(wb_id_dict).length === 0) {
        alert('Please select at least one basin in the map before subsetting');
        return;
    }
    console.log('subsetting');
    document.getElementById('subset-button').disabled = true;
    document.getElementById('subset-loading').style.visibility = "visible";
    const startTime = performance.now(); // Start the timer

    fetch('/subset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(wb_id_dict),
    })
        .then(response => response.text())
        .then(filename => {
            console.log(filename);
            const endTime = performance.now(); // Stop the timer
            const duration = endTime - startTime; // Calculate the duration in milliseconds
            console.log('Request took ' + duration / 1000 + ' milliseconds');
            document.getElementById('output-path').innerHTML = "Done in " + duration / 1000 + "s, subset to <a href='file:///" + filename + "'>" + filename + "</a>";
        })
        .catch(error => {
            console.error('Error:', error);
        }).finally(() => {
            document.getElementById('subset-button').disabled = false;
            document.getElementById('subset-loading').style.visibility = "hidden";
        });
}


async function subset_to_file() {
    if (Object.keys(wb_id_dict).length === 0) {
        alert('Please select at least one basin in the map before subsetting');
        return;
    }
    console.log('subsetting to file');
    document.getElementById('subset-to-file-button').disabled = true;
    document.getElementById('subset-to-file-loading').style.visibility = "visible";
    const startTime = performance.now(); // Start the timer

    fetch('/subset_to_file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(wb_id_dict),
    })
        .then(response => response.text())
        .then(filename => {
            console.log(filename);
            const endTime = performance.now(); // Stop the timer
            const duration = endTime - startTime; // Calculate the duration in milliseconds
            console.log('Request took ' + duration / 1000 + ' milliseconds');
            document.getElementById('output-path').innerHTML = "Done in " + duration / 1000 + "s, subset to <a href='file:///" + filename + "'>" + filename + "</a>";
        })
        .catch(error => {
            console.error('Error:', error);
        }).finally(() => {
            document.getElementById('subset-to-file-button').disabled = false;
            document.getElementById('subset-to-file-loading').style.visibility = "hidden";
        });
}

async function forcings() {
    if (document.getElementById('output-path').textContent === '') {
        alert('Please subset the data before getting forcings');
        return;
    }
    console.log('getting forcings');
    document.getElementById('forcings-button').disabled = true;
    document.getElementById('forcings-loading').style.visibility = "visible";

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
    }).then(response => response.text())
        .then(response_code => {
            document.getElementById('forcings-output-path').textContent = "forcings " + response_code;
        })
        .catch(error => {
            console.error('Error:', error);
        }).finally(() => {
            document.getElementById('forcings-button').disabled = false;
            document.getElementById('forcings-loading').style.visibility = "hidden";

        });
}

async function realization() {
    if (document.getElementById('output-path').textContent === '') {
        alert('Please subset the data before getting a realization');
        return;
    }
    console.log('getting realization');
    document.getElementById('realization-button').disabled = true;
    const forcing_dir = document.getElementById('output-path').textContent;
    const start_time = document.getElementById('start-time').value;
    const end_time = document.getElementById('end-time').value;
    if (forcing_dir === '' || start_time === '' || end_time === '') {
        alert('Please enter a valid output path, start time, and end time');
        document.getElementById('time-warning').style.color = 'red';
        return;
    }
    fetch('/realization', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time }),
    }).then(response => response.text())
        .then(response_code => {
            document.getElementById('realization-output-path').textContent = "realization " + response_code;
        })
        .catch(error => {
            console.error('Error:', error);
        }).finally(() => {
            document.getElementById('realization-button').disabled = false;
        });
}

// These functions are exported by data_processing.js
document.getElementById('subset-button').addEventListener('click', subset);
document.getElementById('subset-to-file-button').addEventListener('click', subset_to_file);
document.getElementById('forcings-button').addEventListener('click', forcings);
document.getElementById('realization-button').addEventListener('click', realization);