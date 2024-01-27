import multiprocessing
import os
from datetime import datetime
from functools import partial
from pathlib import Path

import geopandas as gpd
import pandas as pd
import s3fs
import xarray
import xarray as xr
from exactextract import exact_extract


def get_zarr_stores(start_time, end_time):
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


def get_gdf(geopackage_path, projection):
    gdf = gpd.read_file(geopackage_path, layer="divides")
    gdf = gdf.to_crs(projection)
    return gdf


def clip_stores_to_catchments(store, bounds):
    # clip stores to catchments
    clipped_store = store.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[1], bounds[3]))
    return clipped_store


def compute_store(stores, subset_dir):
    # compute the store
    merged_data = stores.compute()
    # save the store locally
    merged_data.to_netcdf(subset_dir / "merged_data.nc")
    return merged_data


def compute_and_save(rasters, gdf, time, forcings_dir, variable):
    raster = rasters[variable]
    # print(f'Computing {variable} for {time}')
    results = exact_extract(raster, gdf, ["mean"], include_cols=["divide_id"], output="pandas")
    # make divide_id the index
    results = results.set_index("divide_id")
    # make the column name the variable name
    results.columns = [variable]
    if variable == "RAINRATE":
        # this is wrong, todo https://github.com/NOAA-OWP/ngen/issues/509#issuecomment-1504087458
        # should use RAINRATE from previous timestep,         chose this   VVVVV   density factor  at random
        results["APCP_surface"] = (results["RAINRATE"] * 3600 * 1000) / 0.998

    results.to_csv(forcings_dir / f"temp/{variable}_{time}.csv")
    return results


def split_csv_file(file_to_split, timestep, forcings_dir):
    datestring = datetime.strptime(timestep.split("/")[-1].split(".")[0], "%Y%m%d%H%M")
    output_directory = forcings_dir / "by_catchment"
    command = f"""awk -F, 'NR > 1 {{output="{datestring}"; for(i=2; i<=NF; i++) output = output ", " $i; print output >> "{output_directory}/"$1".csv"}}' {file_to_split}"""
    os.system(f"{command}")


def compute_zonal_stats(gdf, merged_data, forcings_dir):
    # desired output format
    # one file per catchment
    # csv with columns: time, LWDOWN, PSFC, Q2D, RAINRATE, SWDOWN, T2D, U2D, V2D
    # exact extract works best with one timestep, lots of polygons
    print("Computing zonal stats")
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
            #   divide_id, LWDOWN,        PSFC,         Q2D,                RAINRATE,    APCP_surface, SWDOWN,        T2D,               U2D,                 V2D
            f.write(
                "time, DLWRF_surface, PRES_surface, SPFH_2maboveground, precip_rate, APCP_surface, DSWRF_surface, TMP_2maboveground, UGRD_10maboveground, VGRD_10maboveground\n"
            )

    # use awk to pivot the csvs to be one per catchment
    for timestep_file in sorted(os.listdir(forcings_dir / "by_time")):
        split_csv_file(forcings_dir / "by_time" / timestep_file, timestep_file, forcings_dir)

    # remove files in the start_map directory
    for file in os.listdir(forcings_dir / "temp"):
        os.remove(forcings_dir / "temp" / file)


def setup_directories(wb_id):
    data_dir = Path(__file__).parent.parent / "output" / wb_id / "config"
    forcings_dir = Path(__file__).parent.parent / "output" / wb_id / "forcings"
    template_file = Path(__file__).parent.parent / "data_sources" / "template.nc"
    geopackage_path = data_dir / f"{wb_id}_subset.gpkg"
    cached_nc_file = Path(__file__).parent.parent / "output" / wb_id / "merged_data.nc"

    for folder in ["by_time", "by_catchment", "temp"]:
        os.makedirs(forcings_dir / folder, exist_ok=True)
    return forcings_dir, geopackage_path, template_file, cached_nc_file


def create_forcings(start_time, end_time, wb_id):
    forcings_dir, geopackage_path, template_file, cached_nc_path = setup_directories(wb_id)
    projection = xr.open_dataset(template_file, engine="netcdf4").crs.esri_pe_string
    print("Got projection from grid file")
    gdf = get_gdf(geopackage_path, projection)
    print("Got gdf")

    if not os.path.exists(cached_nc_path):
        print("no cached nc file found")
        lazy_store = get_zarr_stores(start_time, end_time)
        print("Got zarr stores")
        clipped_store = clip_stores_to_catchments(lazy_store, gdf.total_bounds)
        print("Clipped stores")
        merged_data = compute_store(clipped_store, cached_nc_path)
        print("Computed store")
    merged_data = xr.open_dataset(cached_nc_path)
    compute_zonal_stats(gdf, merged_data, forcings_dir)

if __name__ == "__main__":
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    wb_id = "wb-1643991"
    create_forcings(start_time, end_time, wb_id)
    # takes 9m21s on i9-10850k 20 cores
