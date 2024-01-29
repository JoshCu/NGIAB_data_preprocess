import multiprocessing
import os
from datetime import datetime
from functools import partial
from pathlib import Path
import logging

import geopandas as gpd
import pandas as pd
import s3fs
import xarray
import xarray as xr
from exactextract import exact_extract
from data_processing.file_paths import file_paths
from typing import Tuple


def get_zarr_stores(start_time: str, end_time: str) -> xr.Dataset:
    forcing_zarr_files = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{f}.zarr"
        for f in forcing_zarr_files
    ]
    s3_file_stores = [s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True)) for url in urls]
    time_slice = slice(start_time, end_time)
    lazy_store = xarray.open_mfdataset(
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
    gdf: gpd.GeoDataFrame,
    time: str,
    forcings_dir: Path,
    variable: str,
) -> pd.DataFrame:
    raster = rasters[variable]
    logging.debug(f"Computing {variable} for {time}")
    results = exact_extract(raster, gdf, ["mean"], include_cols=["divide_id"], output="pandas")
    results = results.set_index("divide_id")
    results.columns = [variable]
    if variable == "RAINRATE":
        # this is wrong, todo https://github.com/NOAA-OWP/ngen/issues/509#issuecomment-1504087458
        # should use RAINRATE from previous timestep,chose this density VVVVV factor  at random
        results["APCP_surface"] = (results["RAINRATE"] * 3600 * 1000) / 0.998

    results.to_csv(forcings_dir / f"temp/{variable}_{time}.csv")
    return results


def split_csv_file(file_to_split: Path, timestep: str, forcings_dir: Path) -> None:
    datestring = datetime.strptime(timestep.split("/")[-1].split(".")[0], "%Y%m%d%H%M")
    output_directory = forcings_dir / "by_catchment"
    command = f"""awk -F, 'NR > 1 {{output="{datestring}"; for(i=2; i<=NF; i++) output = output ", " $i; print output >> "{output_directory}/"$1".csv"}}' {file_to_split}"""
    os.system(f"{command}")


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    # desired output format one file per catchment, rows: one per timestep,
    # columns: time, LWDOWN, PSFC, Q2D, RAINRATE, SWDOWN, T2D, U2D, V2D
    # exact extract works best with many timesteps, and as many geometries as possible to reduce parallel overhead

    logging.info("Computing zonal stats")

    # clear by_time files
    for file in os.listdir(forcings_dir / "by_time"):
        os.remove(forcings_dir / "by_time" / file)

    with multiprocessing.Pool() as pool:
        for time in merged_data.time.values:
            raster = merged_data.sel(time=time)
            timestep_result = pool.map(
                partial(compute_and_save, raster, gdf, time, forcings_dir),
                ["LWDOWN", "PSFC", "Q2D", "RAINRATE", "SWDOWN", "T2D", "U2D", "V2D"],
            )
            timestep_result = pd.concat(timestep_result, axis=1)
            timestring = datetime.strftime(pd.to_datetime(str(time)), "%Y%m%d%H%M")
            timestep_result.to_csv(forcings_dir / f"by_time/{timestring}.csv")

    catchment_ids = gdf["divide_id"].unique()

    # clear catchment files
    for file in os.listdir(forcings_dir / "by_catchment"):
        os.remove(forcings_dir / "by_catchment" / file)

    for cat in catchment_ids:
        with open(forcings_dir / f"by_catchment/{cat}.csv", "w") as f:
            # do some renaming here
            # divide_id, LWDOWN, PSFC, Q2D, RAINRATE, APCP_surface, SWDOWN, T2D, U2D, V2D
            f.write(
                "time, DLWRF_surface, PRES_surface, SPFH_2maboveground, precip_rate, APCP_surface, DSWRF_surface, TMP_2maboveground, UGRD_10maboveground, VGRD_10maboveground\n"
            )

    # use awk to pivot the csvs to be one per catchment
    for timestep_file in sorted(os.listdir(forcings_dir / "by_time")):
        split_csv_file(forcings_dir / "by_time" / timestep_file, timestep_file, forcings_dir)

    for file in os.listdir(forcings_dir / "temp"):
        os.remove(forcings_dir / "temp" / file)


def setup_directories(wb_id: str) -> file_paths:
    forcing_paths = file_paths(wb_id)

    for folder in ["by_time", "by_catchment", "temp"]:
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
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir())


if __name__ == "__main__":
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    wb_id = "wb-1643991"
    logging.basicConfig(level=logging.INFO)
    create_forcings(start_time, end_time, wb_id)
    # takes 9m21s on i9-10850k 20 cores
