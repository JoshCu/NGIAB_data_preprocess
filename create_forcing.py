import dask
import numpy
import xarray
import pyproj
import pandas
import geopandas
import s3fs
from dask.distributed import LocalCluster
from geocube.api.core import make_geocube
from pathlib import Path

def get_forcing_zarr_urls():
    forcing_zarr_files = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    return [f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{f}.zarr" for f in forcing_zarr_files]

def merge_zarr_stores(urls, start_time, end_time):
    time_slice = slice(start_time, end_time)
    delayed_stores = [dask.delayed(lambda url: xarray.open_zarr(s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True))).sel(time=time_slice))(url) for url in urls]
    return xarray.merge(dask.compute(*delayed_stores))

def add_spatial_metadata(ds):
    # Spatial metadata processing
    x = ds.x.values
    y = ds.y.values
    X, Y = numpy.meshgrid(x,y)
    template_path = Path(__file__).parent / "data_sources" / "template.nc"
    ds_meta = xarray.open_dataset(template_path)    # CRS and projection transformations
    wrf_proj = pyproj.Proj(proj='lcc',
                       lat_1=30.,
                       lat_2=60., 
                       lat_0=40.0000076293945, lon_0=-97., # Center point
                       a=6370000, b=6370000) 
    # define the output crs
    wgs_proj = pyproj.Proj(proj='latlong', datum='WGS84')

    # transform X, Y into Lat, Lon
    transformer = pyproj.Transformer.from_crs(wrf_proj.crs, wgs_proj.crs)
    lon, lat = transformer.transform(X, Y)
    # Add coordinates and attributes
    ds = ds.assign_coords(lon = (['y', 'x'], lon))
    ds = ds.assign_coords(lat = (['y', 'x'], lat))
    ds = ds.assign_coords(x = x)
    ds = ds.assign_coords(y = y)

    ds.x.attrs['axis'] = 'X'
    ds.x.attrs['standard_name'] = 'projection_x_coordinate'
    ds.x.attrs['long_name'] = 'x-coordinate in projected coordinate system'
    ds.x.attrs['resolution'] = 1000.  # cell size

    ds.y.attrs['axis'] = 'Y' 
    ds.y.attrs['standard_name'] = 'projection_y_coordinate'
    ds.y.attrs['long_name'] = 'y-coordinate in projected coordinate system'
    ds.y.attrs['resolution'] = 1000.  # cell size

    ds.lon.attrs['units'] = 'degrees_east'
    ds.lon.attrs['standard_name'] = 'longitude' 
    ds.lon.attrs['long_name'] = 'longitude'

    ds.lat.attrs['units'] = 'degrees_north'
    ds.lat.attrs['standard_name'] = 'latitude' 
    ds.lat.attrs['long_name'] = 'latitude'

    # add crs to netcdf file
    ds.rio.write_crs(ds_meta.crs.attrs['spatial_ref'], inplace=True
                    ).rio.set_spatial_dims(x_dim="x",
                                                y_dim="y",
                                                inplace=True,
                                            ).rio.write_coordinate_system(inplace=True)
    return ds

def get_subset_geometries(wb_id):
    output_dir = Path(__file__).parent / "output" / wb_id / "config"
    # prepare geometries for spatial averaging
    gdf = geopandas.read_file(output_dir / f'{wb_id.split("_")[0]}_subset.gpkg', layer='divides')

    # convert this data into the projection of our forcing data
    target_crs = pyproj.Proj(proj='lcc',
                        lat_1=30.,
                        lat_2=60., 
                        lat_0=40.0000076293945, lon_0=-97., # Center point
                        a=6370000, b=6370000) 
    gdf = gdf.to_crs(target_crs.crs)
    return gdf

def clip_dataset(ds, gdf):
    gdf['cat'] = gdf.id.str.split('-').str[-1].astype(int)
    # clip retrospective data to the extent of the hydrofabric geometries
    ds = ds.rio.clip(gdf.geometry.values,
                    gdf.crs,
                    drop=True,
                    invert=False)
 
    # create a grid for the geocube
    out_grid = make_geocube(
        vector_data=gdf,
        measurements=["cat"],
        like=ds # ensure the data are on the same grid
    )

    # add the catchment variable to the original dataset
    ds = ds.assign_coords(cat = (['y','x'], out_grid.cat.data))
    print(ds)
    # compute the unique catchment IDs which will be used to compute zonal statistics
    catchment_ids = numpy.unique(ds.cat.data[~numpy.isnan(ds.cat.data)])
    print(ds)
    print(catchment_ids)
    print(f'The dataset contains {len(catchment_ids)} catchments')
    return catchment_ids, ds

# distribute zonal stats to sub processes
def perform_zonal_computation(ds, cat_id):
    ds.cat.compute()
    # subset by catchment id
    ds_catchment = ds.where(ds.cat==cat_id, drop=True)
    delayed = []
    # loop over variables
    for variable in ['LWDOWN', 'PSFC',
                     'Q2D', 'RAINRATE', 'SWDOWN',
                     'T2D', 'U2D', 'V2D']:
                
        delay = dask.delayed(compute_zonal_mean)(ds_catchment[variable], variable)
        delayed.append(delay)
        
    res = dask.compute(*delayed)
    
    # combine outputs (list of dicts) into a single dict.
    res = {k: v for d in res for k, v in d.items()}
    
    # return results
    return {f'cat-{int(cat_id)}': res}

def compute_zonal_mean(ds, variable):   
    return {variable: ds.mean(dim=['x','y']).values}


def compute_zonal_stats(ds, gdf, catchment_ids):
    with LocalCluster().get_client() as client:    
        ds_subset = ds.chunk(chunks={'time': 1000}).unify_chunks().drop_vars(['crs', 'lat', 'lon'])
        scattered_ds = client.scatter(ds_subset, broadcast=True)
        dask.compute(scattered_ds)
        delayed = [dask.delayed(perform_zonal_computation)(scattered_ds, cat_id) for cat_id in catchment_ids]
        results = dask.compute(*delayed)
    return results

def save_data_as_csv(results, start_time, end_time, wb_id, gdf):
    dates = pandas.date_range(start_time, end_time, freq="60min")
    # save the zonal means for each catchment
    for dat in results:
        for cat in dat:
            df = pandas.DataFrame({k:list(v) for k,v in dat[cat].items()})
            df.fillna(0., inplace=True)

            df['APCP_surface'] = df.RAINRATE * 3600

            df.rename(columns={
                'LWDOWN'   : 'DLWRF_surface',
                'PSFC'     : 'PRES_surface',
                'Q2D'      : 'SPFH_2maboveground',
                'SWDOWN'   : 'DSWRF_surface',
                'T2D'      : 'TMP_2maboveground',
                'U2D'      : 'UGRD_10maboveground',
                'V2D'      : 'VGRD_10maboveground',
                'RAINRATE' : 'precip_rate',
            },
                    inplace=True)
                
            # add the time index
            df['time'] = dates
            df.set_index('time', inplace=True)
            column_names = ['APCP_surface',
                                    'DLWRF_surface',
                                    'DSWRF_surface',
                                    'PRES_surface',
                                    'SPFH_2maboveground',
                                    'TMP_2maboveground',
                                    'UGRD_10maboveground',
                                    'VGRD_10maboveground',
                                    'precip_rate']

            # write to file
            curent_dir = Path(__file__).parent
            with open(f'{curent_dir}/output/{wb_id}/forcings/{cat}.csv', 'w') as f:
                # Note: saving "precip_rate" because this column exists in the example 
                #       forcing files. It's not clear if this is being used or not.
                df.to_csv(f,
                        columns = column_names)             

    computed_catchments = [list(r.keys())[0] for r in results]
    for cat_id in gdf['cat'].values:
        known_catchment = f'cat-{int(cat_id)}'
        if known_catchment not in computed_catchments:
            print(f'Creating Synthetic Forcing for {known_catchment}')
            synthetic_df = pandas.DataFrame(0, index=df.index, columns=column_names)
            # write to file
            with open(f'{wb_id}/forcings/{known_catchment}.csv', 'w') as f:
                df.to_csv(f,
                        columns = column_names)

def create_forcings(start_time, end_time, wb_id):
    urls = get_forcing_zarr_urls()
    merged_store = merge_zarr_stores(urls, start_time, end_time)
    ds = add_spatial_metadata(merged_store)
    gdf = get_subset_geometries(wb_id)
    catchment_ids, ds = clip_dataset(ds, gdf)
    results = compute_zonal_stats(ds, gdf, catchment_ids)
    save_data_as_csv(results, start_time, end_time, wb_id, gdf)

if __name__ == '__main__':
    start_time = '2010-01-01 00:00'
    end_time = '2010-01-10 00:00'
    wb_id = 'wb-1643991'
    create_forcings(start_time, end_time, wb_id)
