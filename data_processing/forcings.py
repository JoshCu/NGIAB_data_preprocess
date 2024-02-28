import logging
import multiprocessing
import os
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import pandas as pd
import s3fs
import xarray as xr
from exactextract import exact_extract

from data_processing.file_paths import file_paths

logger = logging.getLogger(__name__)


def load_zarr_datasets(start_time: str, end_time: str) -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    s3_urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr"
        for var in forcing_vars
    ]
    s3_stores = [s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True)) for url in s3_urls]

    dataset = xr.open_mfdataset(s3_stores, combine="by_coords", parallel=True, engine="zarr").sel(
        time=slice(start_time, end_time)
    )
    return dataset


def load_geodataframe(geopackage_path: str, projection: str) -> gpd.GeoDataFrame:
    """Load and project a geodataframe from a given path and projection."""
    gdf = gpd.read_file(geopackage_path, layer="divides").to_crs(projection)
    return gdf


def clip_dataset_to_bounds(
    dataset: xr.Dataset, bounds: Tuple[float, float, float, float]
) -> xr.Dataset:
    """Clip the dataset to specified geographical bounds."""
    return dataset.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[1], bounds[3]))


def compute_store(stores: xr.Dataset, subset_dir: Path) -> xr.Dataset:
    merged_data = stores.compute()
    if file_paths.dev_file().exists():
        merged_data.to_netcdf(subset_dir)
    return merged_data


def compute_and_save(
    rasters: xr.Dataset,
    gdf_chunk: gpd.GeoDataFrame,
    forcings_dir: Path,
    variable: str,
    times: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Compute and save zonal stats for a given variable on a given chunk of the geodataframe.
    This function is called in parallel.
    """
    raster = rasters[variable]
    logger.debug(f"Computing {variable} for all times")
    results = exact_extract(
        raster, gdf_chunk, ["mean"], include_cols=["divide_id"], output="pandas"
    )

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
    logger.info("Computing zonal stats in parallel for all timesteps")
    for file in os.listdir(forcings_dir / "temp"):
        os.remove(forcings_dir / "temp" / file)

    variables = ["LWDOWN", "PSFC", "Q2D", "RAINRATE", "SWDOWN", "T2D", "U2D", "V2D"]
    # compute zonal stats in parallel, chunks should be an 8th of total cpu count
    # computation is done once per chunk per variable aka chunks * variables

    if chunk_size is None:
        # the - signs invert the // floor division operator to round up instead of down
        chunk_size = -(len(variables) * len(gdf) // -multiprocessing.cpu_count())

        if chunk_size < 1:
            # division by zero protection
            chunk_size = 1

    logger.debug(f"Chunk size: {chunk_size}")
    logger.debug(f"CPU count: {multiprocessing.cpu_count()}")
    logger.debug(f"Total divides: {len(gdf)}")

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
                logger.warning(f"File not found for {variable} and {cat}")
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


def create_forcings(start_time: str, end_time: str, output_folder_name: str) -> None:
    forcing_paths = setup_directories(output_folder_name)
    projection = xr.open_dataset(forcing_paths.template_nc(), engine="netcdf4").crs.esri_pe_string
    logger.info("Got projection from grid file")

    gdf = load_geodataframe(forcing_paths.geopackage_path(), projection)
    logger.info("Got gdf")

    if not os.path.exists(forcing_paths.cached_nc_file()):
        logger.info("No cached nc file found")
        logger.info("Loading zarr stores, this may take a while.")
        lazy_store = load_zarr_datasets(start_time, end_time)
        logger.info("Got zarr stores")

        clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds)
        logger.info("Clipped stores")

        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file())
        logger.info("Computed store")
    else:
        merged_data = xr.open_dataset(forcing_paths.cached_nc_file())
        logger.info("Opened cached nc file")

    print(type(merged_data))
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
