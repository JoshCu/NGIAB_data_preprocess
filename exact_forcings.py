import xarray
import pandas
import s3fs
from pathlib import Path
import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.features import rasterize, bounds
from rasterio import transform
from pathlib import Path
from math import floor, ceil
import datetime
import multiprocessing
from functools import partial
import json
from exactextract import exact_extract

def get_zarr_stores(start_time, end_time):
    forcing_zarr_files = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    urls = [f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{f}.zarr" for f in forcing_zarr_files]
    time_slice = slice(start_time, end_time)
    lazy_stores = [xarray.open_zarr(s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True)), drop_variables=['crs']).sel(time=time_slice) for url in urls]
    return lazy_stores

def get_gdf(geopackage_path, projection):
    gdf = gpd.read_file(geopackage_path, layer='divides')
    gdf = gdf.to_crs(projection)
    return gdf

def clip_stores_to_catchments(stores, bounds):
     # clip stores to catchments
    clipped_stores = []
    print(bounds)
    for store in stores:
        clipped_store = store.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[1], bounds[3]))
        clipped_stores.append(clipped_store)
    return clipped_stores

def merge_stores(stores):
    # merge stores
    merged_data = xarray.merge(stores)
    return merged_data

def get_grid_file(grid_file):
    try:
        ds = xr.open_dataset(grid_file,engine='netcdf4')
        try:
            projection = ds.crs.esri_pe_string
        except:
            try:
                projection = ds.ProjectionCoordinateSystem.esri_pe_string
            except:
                raise Exception(f'\n\nCan\'t find projection!\n')
    except:
        raise Exception(f'\n\nThere\'s a problem with {grid_file}!\n')
    print(f'Projection: {projection}')
    return ds, projection

def compute_zonal_stats(gdf, merged_data):
    print('Computing zonal stats')
    print(merged_data)
    ds = merged_data['RAINRATE']
    print(ds.time.values)
    for i in ds.time.values:
        # time stored as numpy.datetime64
        print(i)
        ds_frame = ds.sel(time=i)
        results = exact_extract(ds_frame, gdf, ['mean'], include_cols=['divide_id'])
        print(results)
        break
    with open('results.json', 'w') as f:
        if iter(results):
            for result in results:
                f.write(result)
                f.write('\n')
        else:
            f.write(results)
    return results


def create_forcings(start_time, end_time, wb_id):
    data_directory = Path(__file__).parent / 'output' / wb_id / 'config'
    geopackage_path = data_directory / f'{wb_id}_subset.gpkg'
    template_file = Path(__file__).parent / 'data_sources' / 'template.nc'
    lazy_stores = get_zarr_stores(start_time, end_time)
    df, projection = get_grid_file(template_file)
    gdf = get_gdf(geopackage_path, projection)
    clipped_stores = clip_stores_to_catchments(lazy_stores, gdf.total_bounds)
    merged_data = merge_stores(clipped_stores)
    results = compute_zonal_stats(gdf, merged_data)
    #save_data_as_csv(results, start_time, end_time, wb_id, gdf)


if __name__ == '__main__':
    start_time = '2010-01-01 00:00'
    end_time = '2010-01-10 00:00'
    wb_id = 'wb-1643991'
    create_forcings(start_time, end_time, wb_id)
