document.getElementById('toggleConsole').addEventListener('click', function () {
    var consoleElement = document.getElementById('console');
    consoleElement.classList.toggle('minimized');
    if (consoleElement.classList.contains('minimized')) {
        this.textContent = 'Show Console';
    }
    else {
        this.textContent = 'Hide Console';
    }
});

function fetchLogs() {
    fetch('/logs')
        .then(response => response.json())
        .then(data => {
            var consoleElement = document.getElementById('logOutput');
            consoleElement.innerHTML = data.logs.join('<br>');
            consoleElement.scrollTop = consoleElement.scrollHeight;
        }
        );

}

setInterval(fetchLogs, 1000); // Fetch logs every second
