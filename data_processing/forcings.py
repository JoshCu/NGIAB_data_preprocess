import multiprocessing
import os
from datetime import datetime
from functools import partial
from pathlib import Path
import logging

import geopandas as gpd
import pandas as pd
import s3fs

import xarray as xr
from exactextract import exact_extract
from data_processing.file_paths import file_paths
from typing import Tuple

import pickle


def get_zarr_stores(start_time: str, end_time: str) -> xr.Dataset:
    forcing_zarr_files = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{f}.zarr"
        for f in forcing_zarr_files
    ]
    s3_file_stores = [s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True)) for url in urls]
    time_slice = slice(start_time, end_time)
    lazy_store = xr.open_mfdataset(
        s3_file_stores, combine="by_coords", parallel=True, engine="zarr"
    )
    lazy_store = lazy_store.sel(time=time_slice)
    return lazy_store


def get_gdf(geopackage_path: str, projection: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(geopackage_path, layer="divides")
    gdf = gdf.to_crs(projection)
    return gdf


def clip_stores_to_catchments(
    store: xr.Dataset, bounds: Tuple[float, float, float, float]
) -> xr.Dataset:
    clipped_store = store.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[1], bounds[3]))
    return clipped_store


def compute_store(stores: xr.Dataset, subset_dir: Path) -> xr.Dataset:
    merged_data = stores.compute()
    merged_data.to_netcdf(subset_dir)
    return merged_data


def compute_and_save(
    rasters: xr.Dataset,
    gdf_chunk: gpd.GeoDataFrame,
    forcings_dir: Path,
    variable: str,
    times: pd.DatetimeIndex,
) -> pd.DataFrame:

    raster = rasters[variable]
    logging.debug(f"Computing {variable} for all times")
    results = exact_extract(
        raster, gdf_chunk, ["mean"], include_cols=["divide_id"], output="pandas"
    )
    # Assuming results includes a time dimension now, adjust processing accordingly
    results.set_index("divide_id", inplace=True)

    for i in results.index:
        single_divide = results.loc[i].to_frame()
        # drop first row if needed
        if len(single_divide) != len(times):
            single_divide = single_divide.iloc[1:]
        divide_id = single_divide.columns[0]
        single_divide.columns = [variable]
        # convert row index to timestamp
        single_divide.index = times
        if variable == "RAINRATE":
            single_divide["APCP_surface"] = (single_divide[variable] * 3600 * 1000) / 0.998
        # write to file
        single_divide.to_feather(forcings_dir / f"temp/{variable}_{divide_id}.feather")


def chunk_gdf(gdf, chunk_size):
    """Yield successive n-sized chunks from gdf."""
    for i in range(0, len(gdf), chunk_size):
        yield gdf.iloc[i : i + chunk_size]


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path, chunk_size=None
) -> None:
    logging.info("Computing zonal stats in parallel for all timesteps")
    for file in os.listdir(forcings_dir / "temp"):
        os.remove(forcings_dir / "temp" / file)

    variables = ["LWDOWN", "PSFC", "Q2D", "RAINRATE", "SWDOWN", "T2D", "U2D", "V2D"]
    # compute zonal stats in parallel chunks should be an 8th of total cpu count
    # because there are 8 variables that run in parallel
    if chunk_size is None:
        chunk_size = -(8 * len(gdf) // -multiprocessing.cpu_count())
        if chunk_size < 1:
            chunk_size = 1
    logging.debug(f"Chunk size: {chunk_size}")
    logging.debug(f"CPU count: {multiprocessing.cpu_count()}")
    logging.debug(f"Total divides: {len(gdf)}")

    gdf_chunks = list(chunk_gdf(gdf, chunk_size))

    with multiprocessing.Pool() as pool:
        args = [
            (merged_data, chunk, forcings_dir, variable, merged_data.time.values)
            for chunk in gdf_chunks
            for variable in variables
        ]
        pool.starmap(compute_and_save, args)
    catchment_ids = gdf["divide_id"].unique()

    # clear catchment files
    for file in os.listdir(forcings_dir / "by_catchment"):
        os.remove(forcings_dir / "by_catchment" / file)

    # open and merge the dfs by catchment
    for cat in catchment_ids:
        dfs = []
        for variable in variables:
            try:
                df = pd.read_feather(forcings_dir / f"temp/{variable}_{cat}.feather")
                dfs.append(df)
            except FileNotFoundError:
                logging.warning(f"File not found for {variable} and {cat}")
        if len(dfs) > 0:
            merged_df = pd.concat(dfs, axis=1)
            # rename the columns
            merged_df.columns = [
                "DLWRF_surface",
                "PRES_surface",
                "SPFH_2maboveground",
                "precip_rate",
                "APCP_surface",
                "DSWRF_surface",
                "TMP_2maboveground",
                "UGRD_10maboveground",
                "VGRD_10maboveground",
            ]
            merged_df.index.name = "time"
            merged_df.to_csv(forcings_dir / f"by_catchment/{cat}.csv")

    for file in os.listdir(forcings_dir / "temp"):
        os.remove(forcings_dir / "temp" / file)
    os.rmdir(forcings_dir / "temp")


def setup_directories(wb_id: str) -> file_paths:
    forcing_paths = file_paths(wb_id)

    for folder in ["by_catchment", "temp"]:
        os.makedirs(forcing_paths.forcings_dir() / folder, exist_ok=True)
    return forcing_paths


def create_forcings(start_time: str, end_time: str, wb_id: str) -> None:
    forcing_paths = setup_directories(wb_id)
    projection = xr.open_dataset(forcing_paths.template_nc(), engine="netcdf4").crs.esri_pe_string
    logging.info("Got projection from grid file")

    gdf = get_gdf(forcing_paths.geopackage_path(), projection)
    logging.info("Got gdf")

    if not os.path.exists(forcing_paths.cached_nc_file()):
        logging.info("No cached nc file found")
        lazy_store = get_zarr_stores(start_time, end_time)
        logging.info("Got zarr stores")

        clipped_store = clip_stores_to_catchments(lazy_store, gdf.total_bounds)
        logging.info("Clipped stores")

        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file())
        logging.info("Computed store")

    merged_data = xr.open_dataset(forcing_paths.cached_nc_file())
    print(type(merged_data))
    logging.info(f"Opened cached nc file: [{forcing_paths.cached_nc_file()}]")
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir())


if __name__ == "__main__":
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    wb_id = "wb-1643991"
    logging.basicConfig(level=logging.INFO)
    create_forcings(start_time, end_time, wb_id)
    # takes 9m21s on i9-10850k 20 cores
