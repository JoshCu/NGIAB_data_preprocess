import logging
import sqlite3
from functools import cache
from pathlib import Path
from typing import List, Tuple

import geopandas as gpd

from data_processing.file_paths import file_paths
from shapely.wkb import loads

logger = logging.getLogger(__name__)


def copy_rTree_tables(
    table: str, ids: List[str], source_db: sqlite3.Connection, dest_db: sqlite3.Connection
) -> None:
    """
    Copy rTree tables from source database to destination database.
    This contains the spatial index for the specified table.
    Copying it saves us from having to rebuild the index.
    Args:
        table (str): The table name.
        ids (List[str]): The list of IDs.
        source_db (sqlite3.Connection): The source database connection.
        dest_db (sqlite3.Connection): The destination database connection.
    """
    rTree_tables = [f"rtree_{table}_geom{suffix}" for suffix in ["", "_rowid", "_node", "_parent"]]

    rowid_data = source_db.execute(
        f"SELECT * FROM {rTree_tables[1]} WHERE rowid in ({','.join(ids)})"
    ).fetchall()

    node_ids = [str(x[1]) for x in rowid_data]
    node_data = source_db.execute(
        f"SELECT * FROM {rTree_tables[2]} WHERE nodeno in ({','.join(node_ids)})"
    ).fetchall()

    parent_data = source_db.execute(
        f"SELECT * FROM {rTree_tables[3]} WHERE nodeno in ({','.join(node_ids)})"
    ).fetchall()

    insert_data(dest_db, rTree_tables[2], node_data)
    insert_data(dest_db, rTree_tables[1], rowid_data)
    insert_data(dest_db, rTree_tables[3], parent_data)


def insert_data(con: sqlite3.Connection, table: str, contents: List[Tuple]) -> None:
    """
    Insert data into the specified table.

    Args:
        con (sqlite3.Connection): The database connection.
        table (str): The table name.
        contents (List[Tuple]): The data to be inserted.
    """
    if len(contents) == 0:
        return

    logger.info(f"Inserting {table}")
    placeholders = ",".join("?" * len(contents[0]))
    con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", contents)
    con.commit()


def subset_table(table: str, ids: List[str], hydrofabric: str, subset_gpkg_name: str) -> None:
    """
    Subset the specified table from the hydrofabric database and save it to the subset geopackage.

    Args:
        table (str): The table name.
        ids (List[str]): The list of IDs.
        hydrofabric (str): The path to the hydrofabric database.
        subset_gpkg_name (str): The name of the subset geopackage.
    """
    if table == "flowpath_edge_list":
        table = "network"

    logger.info(f"Subsetting {table} in {subset_gpkg_name}")
    source_db = sqlite3.connect(hydrofabric)
    dest_db = sqlite3.connect(subset_gpkg_name)

    if table == "nexus":
        sql_query = f"SELECT toid FROM divides"
        contents = dest_db.execute(sql_query).fetchall()
        ids = [str(x[0]) for x in contents]

    ids = [f"'{x}'" for x in ids]
    sql_query = f"SELECT * FROM {table} WHERE id IN ({','.join(ids)})"
    contents = source_db.execute(sql_query).fetchall()

    ids = [str(x[0]) for x in contents]

    if table in ["divides", "flowpaths", "nexus", "hydrolocations", "lakes"]:
        copy_rTree_tables(table, ids, source_db, dest_db)

    logger.info("Inserting final data")

    if table == "network":
        table = "flowpath_edge_list"

    insert_data(dest_db, table, contents)
    dest_db.commit()
    source_db.close()
    dest_db.close()

def blob_to_geometry(blob):
    # from http://www.geopackage.org/spec/#gpb_format
    # byte 0-2 don't need
    # byte 3 bit 0 (bit 24)= 0 for little endian, 1 for big endian (used for srs id and envelope type)
    # byte 3 bit 1-3 (bit 25-27)= envelope type (needed to calculate envelope size)
    # byte 3 bit 4 (bit 28)= empty geometry flag
    envelope_type = (blob[3] & 14) >> 1
    empty = (blob[3] & 16) >> 4
    if empty:
        return None
    envelope_sizes = [0, 32, 48, 48, 64]
    envelope_size = envelope_sizes[envelope_type]
    header_byte_length = 8 + envelope_size
    # everything after the header is the geometry
    geom = blob[header_byte_length:]
    # convert to hex
    geometry = loads(geom)
    return geometry

def get_points_from_wbids(wbids: List[str]) -> dict:
    """
    Get the points from the specified wbids.

    Args:
        wbids (List[str]): The list of wbids.

    Returns:
        gpd.GeoDataFrame: The points from the specified wbids.
    """
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    wbids_list = [f"'{x}'" for x in wbids]
    query = f"SELECT id, geom FROM divides WHERE id IN ({','.join(wbids_list)})"
    query = query.replace(",)", ")")
    # print(f"Executing query: {query}")
    data = db.execute(query).fetchall()
    print(f"Got data: {len(data)}")
    db.close()
    wb_centroids = {}
    for i, d in enumerate(data):
        try:
            wb_centroids[d[0]] = blob_to_geometry(d[1]).centroid
            # print(f"Got geometry: {wb_centroids[d[0]]}",end="\r")
        except Exception as e:
            print(f"Error getting geometry for {d[0]}: {e}",end="\r")
            logger.error(f"Error getting geometry for {d[0]}: {e}")
            raise e
    # raise Exception(wb_centroids)
    return wb_centroids





def remove_triggers(dest_db: str) -> List[Tuple]:
    """
    Remove triggers from the specified database.
    As they break any inserts we don't remove them.
    Args:
        dest_db (str): The path to the destination database.

    Returns:
        List[(t_name, t_sql)]: The list of triggers that were removed.
    """
    con = sqlite3.connect(dest_db)
    triggers = con.execute("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'").fetchall()

    for trigger in triggers:
        con.execute(f"DROP TRIGGER {trigger[0]}")

    con.commit()
    con.close()
    return triggers


def add_triggers(triggers: List[Tuple], dest_db: str) -> None:
    """
    Add triggers to the specified database.

    Args:
        triggers (List[Tuple]): The list of triggers to be added.
        dest_db (str): The path to the destination database.
    """
    con = sqlite3.connect(dest_db)

    for trigger in triggers:
        con.execute(trigger[1])

    con.commit()
    con.close()


@cache
def get_vpu_gdf():
    vpu_boundaries = gpd.read_file(
        file_paths.data_sources() / "vpu_boundaries.shp", engine="pyogrio"
    )
    vpu_boundaries = vpu_boundaries.to_crs(epsg=4326)
    return vpu_boundaries
