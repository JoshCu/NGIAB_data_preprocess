# DEMO Tools for NGIAB data preparation

### This is an early version, help us improve it.
This is still in development, your feedback and patience is appreciated.
If you have any suggestions to improve or find bugs that need fixing, submit an issue here on github.

![map screenshot](./map_app/static/resources/screenshot.png)

## Aproximate workflow
1) Select the water basins you're interested in on the map
1) Click subset, this creates a geopackage with the waterbains you've selected + any basin upstream of it
1) Pick a date and time
1) Generate forcings for your water basins
1) Create a cfe realisation for your selected data

# Running with docker and devcontainers
The easiest way to get this all working is with [dev containers](https://code.visualstudio.com/docs/devcontainers/containers).     
It's a docker container managed by vscode:   
1) Clone this repo   
2) Open it in vscode     
3) Click through the popups in the bottom right    
depending on what you've already got installed, it may install wsl, docker, and the vscode devcontainer extension    
4) Wait for it to finish building, view the log to watch it build
5) 
```bash
cd data_sources
wget https://lynker-spatial.s3.amazonaws.com/v20.1/conus.gpkg
wget https://lynker-spatial.s3.amazonaws.com/v20.1/model_attributes.parquet
cd ..
# to run
./run.sh
# the first run may seem slow to start as it needs to generate a river network
```   

#### When using the tool, the output will be ./output/\<your-first-catchment>/
*THERE IS NO OVERWRITE PROTECTION*  
*IT WILL DELETE YOUR OLD OUTPUT FOLDER WHEN YOU CLICK SUBSET*  
*IT WILL DELETE YOU FORCINGS WHEN YOU CLICK CREATE FORCINGS*  
*IT WILL DELETE YOUR REALIZATION WHEN YOU CLICK CREATE REALIZATION*

<details>
    <summary>Manual installation</summary>

## Requirements

* [gdal]() (at least the same version as your gdal pyhon pip package)
* [exact_extract python package](https://github.com/isciences/exactextract.git)


## Native ubuntu (or wsl)
The most up to date steps will be in the 
[dockerfile in the .devcontainer folder](.devcontainer/Dockerfile).



</details>
