#!/bin/sh

# clear the app log
# the file needs to exist and have non whitespace content
echo "Starting Application!" > app.log


# if .dev is present run in debug mode
if [ -f .dev ]; then
    echo "Running in debug mode"
    flask -A map_app run --debug
else
    echo "Running in production mode"
    flask -A map_app run
fi
