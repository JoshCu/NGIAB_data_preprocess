import logging
import multiprocessing
import os
import time
from pathlib import Path
from typing import Tuple
from functools import partial, cache
from datetime import datetime

import numba
import numpy as np
import dask
import geopandas as gpd
import pandas as pd
import s3fs
import xarray as xr
from exactextract import exact_extract

from data_processing.file_paths import file_paths

logger = logging.getLogger(__name__)


@cache
def open_s3_store(url: str) -> s3fs.S3Map:
    """Open an s3 store from a given url."""
    return s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True))


@cache
def load_zarr_datasets() -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    s3_urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr"
        for var in forcing_vars
    ]
    s3_stores = [open_s3_store(url) for url in s3_urls]
    dataset = xr.open_mfdataset(s3_stores, parallel=True, engine="zarr")
    return dataset


def load_geodataframe(geopackage_path: str, projection: str) -> gpd.GeoDataFrame:
    """Load and project a geodataframe from a given path and projection."""
    gdf = gpd.read_file(geopackage_path, layer="divides").to_crs(projection)
    return gdf


def clip_dataset_to_bounds(
    dataset: xr.Dataset, bounds: Tuple[float, float, float, float], start_time: str, end_time: str
) -> xr.Dataset:
    """Clip the dataset to specified geographical bounds."""
    dataset = dataset.sel(
        x=slice(bounds[0], bounds[2]),
        y=slice(bounds[1], bounds[3]),
        time=slice(start_time, end_time),
    )
    logger.info("Selected time range and clipped to bounds")
    return dataset


def compute_store(stores: xr.Dataset, cached_nc_path: Path) -> xr.Dataset:
    if file_paths.dev_file().exists():
        stores.to_netcdf(cached_nc_path)
    data = xr.open_dataset(cached_nc_path, chunks={})
    return data


@numba.njit(parallel=True)
def compute_weighted_mean(data_array, cell_ids, weights):
    mean_at_timestep = np.zeros(data_array.shape[0])
    for time_step in numba.prange(data_array.shape[0]):
        weighted_total = 0.0
        for i in range(cell_ids.shape[0]):
            weighted_total += data_array[time_step][cell_ids[i]] * weights[i]
        mean_at_timestep[time_step] = weighted_total
    return mean_at_timestep


def get_cell_weights(raster, gdf):
    one_timestep = raster.isel(time=0)
    output = exact_extract(
        one_timestep["LWDOWN"],
        gdf,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )
    return output.set_index("divide_id")


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    catchments = get_cell_weights(
        merged_data, gdf
    )  # Assuming this returns a DataFrame or similar structure

    variables = ["LWDOWN", "PSFC", "Q2D", "RAINRATE", "SWDOWN", "T2D", "U2D", "V2D"]

    results = []
    for variable in variables:
        variable_data = []
        for catchment in catchments.index.unique():
            cell_ids = catchments.loc[catchment]["cell_id"]
            weights = catchments.loc[catchment]["coverage"]

            # Flatten spatial dimensions for weighted mean calculation if necessary
            raster = merged_data[variable].data.reshape(merged_data[variable].shape[0], -1)

            mean_at_timesteps = compute_weighted_mean(raster, cell_ids, weights)

            temp_da = xr.DataArray(
                mean_at_timesteps,
                dims=["time"],
                coords={"time": merged_data["time"].values},
                name=f"{variable}_{catchment}",
            )
            temp_da = temp_da.assign_coords(catchment=catchment)
            variable_data.append(temp_da)

        # Concatenate data arrays for each variable across all catchments
        concatenated_da = xr.concat(variable_data, dim="catchment")
        results.append(concatenated_da.to_dataset(name=variable))

    # Combine all variables into a single dataset
    final_ds = xr.merge(results)

    output_folder = forcings_dir / "by_catchment"
    # Clear out any existing files
    for file in output_folder.glob("*.csv"):
        file.unlink()

    # Save to disk
    delayed_saves = []
    for catchment in final_ds.catchment.values:
        catchment_ds = final_ds.sel(catchment=catchment)
        csv_path = output_folder / f"{catchment}.csv"
        delayed_save = dask.delayed(catchment_ds.to_dataframe().to_csv(csv_path))
        delayed_saves.append(delayed_save)

    dask.compute(*delayed_saves)

    logger.info(f"Zonal stats computed in {time.time() - timer_start} seconds")


def setup_directories(wb_id: str) -> file_paths:
    forcing_paths = file_paths(wb_id)

    for folder in ["by_catchment", "temp"]:
        os.makedirs(forcing_paths.forcings_dir() / folder, exist_ok=True)
    return forcing_paths


def create_forcings(start_time: str, end_time: str, output_folder_name: str) -> None:
    forcing_paths = setup_directories(output_folder_name)
    projection = xr.open_dataset(forcing_paths.template_nc(), engine="netcdf4").crs.esri_pe_string
    logger.info("Got projection from grid file")

    gdf = load_geodataframe(forcing_paths.geopackage_path(), projection)
    logger.info("Got gdf")

    if type(start_time) == datetime:
        start_time = start_time.strftime("%Y-%m-%d %H:%M")
    if type(end_time) == datetime:
        end_time = end_time.strftime("%Y-%m-%d %H:%M")

    if not os.path.exists(forcing_paths.cached_nc_file()):
        logger.info("No cached nc file found")
        logger.info("Loading zarr stores, this may take a while.")
        lazy_store = load_zarr_datasets()
        logger.info("Got zarr stores")

        clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds, start_time, end_time)
        logger.info("Clipped stores")

        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file())
        logger.info("Computed store")
    else:
        merged_data = xr.open_dataset(forcing_paths.cached_nc_file())
        logger.info("Opened cached nc file")

    # print(type(merged_data))
    logger.info(f"Opened cached nc file: [{forcing_paths.cached_nc_file()}]")
    logger.info("Computing zonal stats")
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir())


if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    output_folder_name = "wb-1643991"
    # looks in output/wb-1643991/config for the geopackage wb-1643991_subset.gpkg
    # puts forcings in output/wb-1643991/forcings
    logger.basicConfig(level=logging.DEBUG)
    create_forcings(start_time, end_time, output_folder_name)
