import json
import os
import shutil
import sqlite3
from pathlib import Path

import geopandas as gpd
import igraph as ig
import pandas as pd
import pyarrow.parquet as pq

triggers = None

data_sources = Path(__file__).parent / "data_sources"


def create_graph_from_gpkg(hydrofabric: Path):
    # create graph from geopackage
    sql_query = "SELECT id , toid FROM network WHERE id IS NOT NULL"
    con = sqlite3.connect(str(hydrofabric.absolute()))
    merged = con.execute(sql_query).fetchall()
    con.close()
    [tuple(x) for x in merged]
    merged = list(set(merged))
    print("Building Graph Network with igraph")
    network_graph = ig.Graph.TupleList(merged, directed=True)
    return network_graph


def get_graph(geopackage_path: Path):
    gpickle_path = geopackage_path.with_suffix(".gpickle")
    network_graph = ig.Graph()
    if not gpickle_path.exists():
        # get data needed to construct the graph
        network_graph = create_graph_from_gpkg(geopackage_path)
        # save the graph
        network_graph.write_pickle(gpickle_path)

    network_graph = network_graph.Read_Pickle(gpickle_path)
    print(network_graph.summary())
    return network_graph


def get_upstream_ids(names, graph):
    if type(names) == str:
        names = [names]
    parent_names = []
    for name in names:
        id = graph.vs.find(name=name).index
        parents = graph.subcomponent(id, mode="in")
        # get names of ids in parents
        parent_names.extend([graph.vs[x]["name"] for x in parents])
    return parent_names


def copy_rTree_tables(table, ids, source_db, dest_db):
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
    # geo_data = source_db.execute(f"SELECT * FROM {rTree_tables[0]} WHERE id in ({','.join(ids)})").fetchall()
    insert_data(dest_db, rTree_tables[2], node_data)
    insert_data(dest_db, rTree_tables[1], rowid_data)
    insert_data(dest_db, rTree_tables[3], parent_data)
    # insert_data(dest_db, rTree_tables[0], geo_data)


def insert_data(con, table, contents):
    if len(contents) == 0:
        return
    print(f"Inserting {table}")
    placeholders = ",".join("?" * len(contents[0]))
    con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", contents)
    con.commit()


def subset_table(table, ids, hydrofabric, subset_gpkg_name):
    if table == "flowpath_edge_list":
        table = "network"
    print(f"Subsetting {table} in {subset_gpkg_name}")
    source_db = sqlite3.connect(hydrofabric)
    dest_db = sqlite3.connect(subset_gpkg_name)
    ids = [f"'{x}'" for x in ids]
    # dest_db.enable_load_extension(True)
    # dest_db.load_extension('mod_spatialite')
    # copy selected rows from source to destination
    sql_query = f"SELECT * FROM {table} WHERE id IN ({','.join(ids)})"
    contents = source_db.execute(sql_query).fetchall()
    ids = [str(x[0]) for x in contents]
    if table in ["divides", "flowpaths", "nexus", "hydrolocations", "lakes"]:
        copy_rTree_tables(table, ids, source_db, dest_db)
    # replace ids with new ids
    # new_contents = [[i+1, *x[2:]] for i, x in enumerate(contents)]
    print("inserting final data")
    if table == "network":
        table = "flowpath_edge_list"
    insert_data(dest_db, table, contents)
    dest_db.commit()
    source_db.close()
    dest_db.close()


def remove_triggers(dest_db):
    con = sqlite3.connect(dest_db)
    triggers = con.execute("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'").fetchall()
    for trigger in triggers:
        con.execute(f"DROP TRIGGER {trigger[0]}")
    con.commit()
    con.close()
    return triggers


def add_triggers(triggers, dest_db):
    con = sqlite3.connect(dest_db)
    for trigger in triggers:
        con.execute(trigger[1])
    con.commit()
    con.close()


def create_subset_gpkg(ids, hydrofabric):
    output_dir = Path(__file__).parent / "output" / ids[0]
    output_dir.mkdir(parents=True, exist_ok=True)
    subset_gpkg_name = output_dir / f"{ids[0]}_subset.gpkg"
    if os.path.exists(subset_gpkg_name):
        os.remove(subset_gpkg_name)
    template = Path(__file__).parent / "data_sources" / "template.gpkg"
    print(f"Copying template {template} to {subset_gpkg_name}")
    shutil.copy(template, subset_gpkg_name)
    triggers = remove_triggers(subset_gpkg_name)
    print(f"removed triggers from subset gpkg {subset_gpkg_name}")
    # print(f"triggers removed: {triggers}")
    subset_tables = [
        "divides",
        "nexus",
        "flowpaths",
        "flowpath_edge_list",
        "flowpath_attributes",
        "hydrolocations",
        "lakes",
    ]
    for table in subset_tables:
        subset_table(table, ids, hydrofabric, str(subset_gpkg_name.absolute()))
    add_triggers(triggers, subset_gpkg_name)
    return subset_gpkg_name


def subset_parquet(ids):
    cat_ids = [x.replace("wb", "cat") for x in ids if x.startswith("wb")]
    parquet_file = f"conus_model_attributes.parquet"
    # get absolute path
    parquet_path = Path(__file__).parent.absolute() / "data_sources" / parquet_file
    output_dir = Path(__file__).parent / "output" / ids[0]
    print(str(parquet_path))
    model_attributes = pq.ParquetDataset(str(parquet_path)).read_pandas().to_pandas()
    model_attributes = model_attributes.set_index("divide_id").loc[cat_ids]
    model_attributes.to_csv(output_dir / "cfe_noahowp_attributes.csv")


def make_x_walk(hydrofabric, out_dir) -> None:
    attributes = gpd.read_file(
        hydrofabric, layer="flowpath_attributes", engine="pyogrio"
    ).set_index("id")
    x_walk = pd.Series(attributes[~attributes["rl_gages"].isna()]["rl_gages"])
    data = {}
    for wb, gage in x_walk.items():
        data[wb] = {"Gage_no": [gage]}
    with open(out_dir / "crosswalk.json", "w") as fp:
        json.dump(data, fp, indent=2)


def read_layer(hydrofabric, layer):
    con = sqlite3.connect(str(hydrofabric.absolute()))
    df = pd.read_sql_query(f"SELECT * from {layer}", con)
    con.close()
    return df


def make_geojson(hydrofabric: str) -> None:
    out_dir = Path(__file__).parent / "output" / hydrofabric.stem.replace("_subset", "")
    try:
        catchments = gpd.read_file(hydrofabric, layer="divides", engine="pyogrio")
        nexuses = gpd.read_file(hydrofabric, layer="nexus", engine="pyogrio")
        flowpaths = gpd.read_file(hydrofabric, layer="flowpaths", engine="pyogrio")
        edge_list = gpd.read_file(hydrofabric, layer="flowpath_edge_list", engine="pyogrio")

        make_x_walk(hydrofabric, out_dir)
        catchments.to_file(out_dir / "catchments.geojson")
        nexuses.to_file(out_dir / "nexus.geojson")
        flowpaths.to_file(out_dir / "flowpaths.geojson")
        edge_list.to_json(out_dir / "flowpath_edge_list.json", orient="records", indent=2)
    except Exception as e:
        print(f"Unable to use hydrofabric file {hydrofabric}")
        print(str(e))
        raise e


def subset(hydrofabric: str, wb_ids: list[str]) -> str:
    output_dir = Path(__file__).parent / "output"
    data_sources = Path(__file__).parent / "data_sources"
    hydrofabric = data_sources / hydrofabric
    graph = get_graph(hydrofabric)
    upstream_ids = []
    for id in wb_ids:
        upstream_ids += get_upstream_ids(id, graph)
    upstream_ids = sorted(list(set(upstream_ids)))  # Sort the list
    output_dir = output_dir / upstream_ids[0]
    if output_dir.exists():
        os.system(f"rm -rf {output_dir}")
    gpkg_name = create_subset_gpkg(upstream_ids, hydrofabric)
    output_gpkg = output_dir / gpkg_name
    os.system(f"ogr2ogr -f GPKG {output_dir / 'temp.gpkg'} {output_gpkg}")
    os.system(f"rm {output_gpkg}* && mv {output_dir / 'temp.gpkg'} {output_gpkg}")
    subset_parquet(upstream_ids)
    make_geojson(gpkg_name)
    # make config subfolder and move files there
    config_dir = output_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    # get all files in output_dir
    files = [x for x in output_dir.iterdir()]
    for file in files:
        if file.suffix in [".gpkg", ".csv", ".json", ".geojson"]:
            os.system(f"mv {file} {config_dir}")
    return str(output_dir.absolute())


if __name__ == "__main__":
    output_dir = Path(__file__).parent / "output"
    data_sources = Path(__file__).parent / "data_sources"
    hydrofabric = data_sources / "conus.gpkg"
    print("Getting Graph")
    graph = get_graph(hydrofabric)
    print("Getting Upstream IDs")
    upstream_ids = get_upstream_ids("wb-1643991", graph)
    output_dir = output_dir / upstream_ids[0]
    if output_dir.exists():
        os.system(f"rm -rf {output_dir}")
    print("Creating Subset GPKG")
    gpkg_name = create_subset_gpkg(upstream_ids, hydrofabric)
    output_gpkg = output_dir / gpkg_name
    os.system(f"ogr2ogr -f GPKG {output_dir / 'temp.gpkg'} {output_gpkg}")
    os.system(f"rm {output_gpkg}* && mv {output_dir / 'temp.gpkg'} {output_gpkg}")
    subset_parquet(upstream_ids)
    make_geojson(gpkg_name)
    # make config subfolder and move files there
    config_dir = output_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    # get all files in output_dir
    files = [x for x in output_dir.iterdir()]
    for file in files:
        if file.suffix in [".gpkg", ".csv", ".json", ".geojson"]:
            os.system(f"mv {file} {config_dir}")
