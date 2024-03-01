import json
import os
import shutil
import logging
from typing import List
import geopandas as gpd
import pandas as pd
import pyarrow
from pyarrow import csv as pa_csv, parquet as pa_parquet, compute as pa_compute

from pathlib import Path
from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import add_triggers, remove_triggers, subset_table
from data_processing.graph_utils import get_upstream_ids


logger = logging.getLogger(__name__)


def create_subset_gpkg(ids: List[str], hydrofabric: str) -> Path:
    output_dir = file_paths.root_output_dir() / ids[0]
    output_dir.mkdir(parents=True, exist_ok=True)
    subset_gpkg_name = output_dir / f"{ids[0]}_subset.gpkg"

    if os.path.exists(subset_gpkg_name):
        os.remove(subset_gpkg_name)

    template = file_paths.template_gpkg()
    logger.info(f"Copying template {template} to {subset_gpkg_name}")
    shutil.copy(template, subset_gpkg_name)

    triggers = remove_triggers(subset_gpkg_name)
    logger.info(f"Removed triggers from subset gpkg {subset_gpkg_name}")

    subset_tables = [
        "divides",
        "nexus",
        "flowpaths",
        "flowpath_edge_list",
        "flowpath_attributes",
        "hydrolocations",
        # Commented out for v20.1 gpkg
        # "lakes",
    ]

    for table in subset_tables:
        subset_table(table, ids, hydrofabric, str(subset_gpkg_name.absolute()))

    add_triggers(triggers, subset_gpkg_name)
    return subset_gpkg_name


def subset_parquet(ids: List[str]) -> None:
    cat_ids = [x.replace("wb", "cat") for x in ids]
    parquet_path = file_paths.parquet()
    output_dir = file_paths.root_output_dir() / ids[0]
    logger.debug(str(parquet_path))
    logger.info("Reading parquet")
    table = pa_parquet.read_table(parquet_path)
    logger.info("Filtering parquet")
    filtered_table = table.filter(
        pa_compute.is_in(table.column("divide_id"), value_set=pyarrow.array(cat_ids))
    )
    logger.info("Writing parquet")
    pa_csv.write_csv(filtered_table, output_dir / "cfe_noahowp_attributes.csv")


def make_x_walk(hydrofabric: str, out_dir: str) -> None:
    attributes = gpd.read_file(
        hydrofabric, layer="flowpath_attributes", engine="pyogrio"
    ).set_index("id")
    x_walk = pd.Series(attributes[~attributes["rl_gages"].isna()]["rl_gages"])
    data = {}
    for wb, gage in x_walk.items():
        data[wb] = {"Gage_no": [gage]}
    with open(out_dir / "crosswalk.json", "w") as fp:
        json.dump(data, fp, indent=2)


def make_geojson(hydrofabric: Path) -> None:
    out_dir = file_paths.root_output_dir() / hydrofabric.stem.replace("_subset", "")
    try:
        catchments = gpd.read_file(hydrofabric, layer="divides", engine="pyogrio")
        nexuses = gpd.read_file(hydrofabric, layer="nexus", engine="pyogrio")
        flowpaths = gpd.read_file(hydrofabric, layer="flowpaths", engine="pyogrio")
        edge_list = gpd.read_file(hydrofabric, layer="flowpath_edge_list", engine="pyogrio")

        catchments = catchments.rename(columns={"id": "wb_id"})
        catchments = catchments.rename(columns={"divide_id": "id"})

        make_x_walk(hydrofabric, out_dir)
        catchments.to_file(out_dir / "catchments.geojson")
        nexuses.to_file(out_dir / "nexus.geojson")
        flowpaths.to_file(out_dir / "flowpaths.geojson")
        edge_list.to_json(out_dir / "flowpath_edge_list.json", orient="records", indent=2)
    except Exception as e:
        logger.error(f"Unable to use hydrofabric file {hydrofabric}")
        logger.error(str(e))
        raise e


def subset(wb_ids: List[str], hydrofabric: str = file_paths.conus_hydrofabric()) -> str:
    if isinstance(wb_ids, str):
        wb_ids = [wb_ids]

    upstream_ids = get_upstream_ids(wb_ids)
    upstream_ids = sorted(list(upstream_ids))
    subset_output_dir = file_paths.root_output_dir() / upstream_ids[0]
    remove_existing_output_dir(subset_output_dir)

    gpkg_name = create_subset_gpkg(upstream_ids, hydrofabric)
    output_gpkg = subset_output_dir / gpkg_name

    convert_gpkg_to_temp(subset_output_dir, output_gpkg)
    subset_parquet(upstream_ids)
    make_geojson(gpkg_name)

    move_files_to_config_dir(subset_output_dir)
    logger.info(f"Subset complete for {upstream_ids}")
    return str(subset_output_dir.absolute())


def remove_existing_output_dir(subset_output_dir: str) -> None:
    if subset_output_dir.exists():
        os.system(f"rm -rf {subset_output_dir / 'config'}")
        os.system(f"rm -rf {subset_output_dir / 'forcings'}")


def convert_gpkg_to_temp(subset_output_dir: str, output_gpkg: str) -> None:
    os.system(f"ogr2ogr -f GPKG {subset_output_dir / 'temp.gpkg'} {output_gpkg}")
    os.system(f"rm {output_gpkg}* && mv {subset_output_dir / 'temp.gpkg'} {output_gpkg}")


def move_files_to_config_dir(subset_output_dir: str) -> None:
    config_dir = subset_output_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    files = [x for x in subset_output_dir.iterdir()]
    for file in files:
        if file.suffix in [".gpkg", ".csv", ".json", ".geojson"]:
            os.system(f"mv {file} {config_dir}")
