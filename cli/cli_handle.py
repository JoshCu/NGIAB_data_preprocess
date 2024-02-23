import data_processing.subset as subset
import data_processing.graph_utils as graph_utils
import data_processing.forcings as forcings
import data_processing.gpkg_utils as gpkg_utils
import data_processing.file_paths as file_paths
import data_processing.create_realization as realization
import sys, os
from datetime import datetime

# File for methods related to handling the command line interface

#Subset interface
# ids: list[str]
# returns: None
def subset_interface(ids: list[str]) -> None:
    subset_geopackage = subset.subset(ids)
    print(f"Subset geopackage created at {subset_geopackage}")

#Forcings interface
# ids: list[str]
# config: dict
# returns: None
def forcings_interface(ids: list[str], config: dict) -> None:
    start_time = datetime.strptime(config["start_time"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(config["end_time"], "%Y-%m-%dT%H:%M")
    wb_id = config["forcing_dir"]
    forcings.create_forcings(start_time, end_time, wb_id)
    print(f"Forcings created for {wb_id}")

#Realization interface
# ids: list[str]
# config: dict
# returns: None
def realization_interface(ids: list[str], config: dict) -> None:
    start_time = datetime.strptime(config["start_time"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(config["end_time"], "%Y-%m-%dT%H:%M")
    wb_id = config["forcing_dir"]
    realization.create_cfe_wrapper(wb_id, start_time, end_time)
    print(f"Realization created for {wb_id}")
    