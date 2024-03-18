import os
import sys
from datetime import datetime

import data_processing.create_realization as realization
import data_processing.file_paths as fp
file_paths = fp.file_paths
import data_processing.forcings as forcings
import data_processing.gpkg_utils as gpkg_utils
import data_processing.graph_utils as graph_utils
import data_processing.subset as subset
import matplotlib.pyplot as plt
import igraph as ig
import sqlite3
import time
from multiprocessing import Pool, cpu_count, Value
import signal
from shapely import unary_union, Geometry
from math import log2

from geopandas import GeoDataFrame
import pickle
from pathlib import Path
import random
# File for methods related to handling the command line interface

ALLOW_FILE_CACHE = False
SUBSET_CACHE_THRESHOLD = 50000
PARTITION_THRESHOLD = 10000

#Subset interface
# ids: list[str]
# returns: None
def subset_interface(ids: list[str]) -> None:
    subset_geopackage = subset.subset(ids)
    print(f"Subset geopackage created at {subset_geopackage}")

#Forcings interface
# ids: list[str]
# config: dict
# returns: None
def forcings_interface(ids: list[str], config: dict) -> None:
    start_time = datetime.strptime(config["start_time"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(config["end_time"], "%Y-%m-%dT%H:%M")
    wb_id = config["forcing_dir"]
    forcings.create_forcings(start_time, end_time, wb_id)
    print(f"Forcings created for {wb_id}")

#Realization interface
# ids: list[str]
# config: dict
# returns: None
def realization_interface(ids: list[str], config: dict) -> None:
    start_time = datetime.strptime(config["start_time"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(config["end_time"], "%Y-%m-%dT%H:%M")
    wb_id = config["forcing_dir"]
    realization.create_cfe_wrapper(wb_id, start_time, end_time)
    print(f"Realization created for {wb_id}")

#Utility function to safely truncate dataset
#if the dataset is larger than desired, we would like to be able to
# choose a smaller dataset without compromising adjacency
# ids: list[str]
# ratio: float (optional, 0 to 1 or None) default: None
# num: int (optional, 0 to len(ids) or None) default: None
# returns: list[str]
def safe_truncate(ids: list[str], ratio: float = None, num: int = None) -> list[str]:
    if ratio is not None and (ratio < 0 or ratio > 1):
        raise ValueError(f"Ratio must be between 0 and 1, not {ratio}")
    if num is not None and (num < 0 or num > len(ids)):
        raise ValueError(f"Num must be between 0 and the length of the dataset, not {num}")
    if ratio is not None and num is not None:
        raise ValueError("Cannot specify both ratio and num")
    original_len = len(ids)
    desired_len = original_len
    if ratio is not None:
        desired_len = int(original_len * ratio)
    if num is not None:
        desired_len = num
    if desired_len >= original_len:
        #No need to truncate
        return ids
    #Begin truncation
    graph = graph_utils.get_graph()
    subgraph, tnx_subgraphs, ntnx_subgraphs = _create_groups(ids)
    # print(f"Subgraph has {len(subgraph)}")
    #plot the subgraph
    # fig, ax = plt.subplots()
    # subgraph.__plot__("matplotlib", ax)
    # plt.show()
    sizes = [len(s.vs) for s in subgraph]
    print(f"Sizes: {sizes}")
    tnx_sizes = [len(s.vs) for s in tnx_subgraphs]
    print(f"TNX Sizes: {tnx_sizes}")
    ntnx_sizes = [len(s.vs) for s in ntnx_subgraphs]
    print(f"NTNX Sizes: {ntnx_sizes}")
    with open(file_paths.root_output_dir() / "ntnx_subset.txt", "w") as f:
        ntnx_names = []
        for s in ntnx_subgraphs:
            ntnx_names.extend([v["name"] for v in s.vs])
        ntnx_wbids = sorted([n for n in ntnx_names if "wb" in n])
        f.write("\n".join(ntnx_wbids))



def _create_groups(ids: list[str]):# -> list[set[str]]:
    graph = graph_utils.get_graph()
    verts = [graph.vs.find(name=id) for id in ids]
    print(f"{len(ids)} ids -> {len(verts)} vertices")
    vset = set([v.index for v in verts])
    for v in verts:
        successors = v.successors()
        for s in successors:
            vset.add(s.index)
    print(f"Subgraph has {len(vset)} vertices")
    subgraph = _create_subgraph_from_vertex_set(graph, vset)
    subgraphs = subgraph.decompose(minelements=1)
    print(f"Subgraph has {len(subgraphs)} subgraphs")
    sgraph_has_tnx = lambda s: len(s.vs.select(lambda v: "tnx" in v["name"])) > 0
    tnx_subgraphs = [s for s in subgraphs if sgraph_has_tnx(s)]
    ntnx_subgraphs = [s for s in subgraphs if not sgraph_has_tnx(s)]
    print(f"Subgraph has {len(tnx_subgraphs)} subgraphs with tnx")
    print(f"Subgraph has {len(ntnx_subgraphs)} subgraphs without tnx")
    return subgraphs, tnx_subgraphs, ntnx_subgraphs

def _create_subgraph_from_vertex_set(graph: ig.Graph, vset: set[int]) -> ig.Graph:
    total = len(vset)
    percent = lambda x: f"{x/total*100:.2f}%"
    new_graph = ig.Graph(directed=True)
    vnames = {v: graph.vs[v]["name"] for v in vset}
    vnew_ids = {v: new_graph.add_vertex(name=vnames[v]) for v in vset}
    vtouched = {v: False for v in vset}
    edges = []
    for i, v in enumerate(vset):
        
        succ = [s for s in graph.vs[v].successors() if s.index in vnames]
        # pred = [s for s in graph.vs[v].predecessors() if s.index in vnames]
        edges.extend([(vnew_ids[v], vnew_ids[s.index]) for s in succ])
        # edges.extend([(vnew_ids[v], vnew_ids[s.index]) for s in graph.vs[v].successors() if s.index in vnames])
        # edges.extend([(vnew_ids[s.index], vnew_ids[v]) for s in graph.vs[v].predecessors() if s.index in vnames and not vtouched[s.index]])
        print(f"Adding edges: i:{percent(i)}, len: {len(edges)}", end="\r")#. S({len(new_graph.subcomponent(vnew_ids[v], 'all'))})",end="\r")
        vtouched[v] = True
        # edges.extend([(vnew_ids[v], vnew_ids[s.index]) for s in graph.vs[v].successors() if s.index in vnames])
    new_graph.add_edges(edges)
    print(f"Added edges: {percent(total)}")
    print(f"Vertices touched: {sum(vtouched.values())}/{total}")
    return new_graph

def _all_wbids():
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    data = db.execute("SELECT id FROM divides").fetchall()
    db.close()
    return set([d[0] for d in data if isinstance(d[0], str) and "wb" in d[0]])

def _check_pickle(fname:str, path:str = file_paths.root_output_dir()):
    global ALLOW_FILE_CACHE
    if not ALLOW_FILE_CACHE:
        return False
    if isinstance(path, str):
        path = Path(path)
    if not "." in fname:
        fname += ".pickle"
    path = path / fname
    if path.exists() and path.is_file():
        return True
    return None

def _get_pickle(fname:str, path:str = file_paths.root_output_dir()):
    global ALLOW_FILE_CACHE
    if not ALLOW_FILE_CACHE:
        return None
    if isinstance(path, str):
        path = Path(path)
    if not "." in fname:
        fname += ".pickle"
    path = path / fname
    if path.exists() and path.is_file():
        return pickle.load(open(path, "rb"))
    return None

def _send_pickle(data, fname:str, path:str = file_paths.root_output_dir()) -> None:
    global SUBSET_CACHE_THRESHOLD
    splits = fname.split("_")
    if len(splits) > 1 and "wb" in splits[-1]:
        _size = int(splits[-2])
        if _size < SUBSET_CACHE_THRESHOLD:
            return
    if isinstance(path, str):
        path = Path(path)
    if not "." in fname:
        fname += ".pickle"
    path = path / fname
    pickle.dump(data, open(path, "wb"))

def _mp_init_shared_mem(_chunks_done, _items_done, _total_items):
    global chunks_done, items_done, total_items
    chunks_done, items_done, total_items = _chunks_done, _items_done, _total_items

def _mp_init_shared_mem_generic(_data):
    global data
    data = {}
    data.update(_data)

def _mp_create_wbid_map(ctx):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    subset_index = ctx[0]
    subgraph = ctx[1]
    wbids = [v["name"] for v in subgraph.vs]
    partial_map = {}
    for wb in wbids:
        try:
            partial_map[wb] = subset_index
        except Exception as e:
            print(f"Error adding {wb} to map: {e}", end="\n")
            raise e
    chunks_done.value += 1
    items_done.value += len(wbids)
    print(f"Processed {chunks_done.value} chunks, {float(items_done.value)/total_items.value*100:.2f}%", end="\r")
    return partial_map

def _mp_merge_wbid_maps(ctx):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    map1 = ctx[0][0]
    for i in range(1, len(ctx[0])):
        map1.update(ctx[0][i])
    chunks_done.value += 1
    items_done.value += len(map1)
    print(f"Merged {chunks_done.value} chunks, {float(items_done.value)/total_items.value*100:.2f}%", end="\r")
    return map1

def _mp_subset_dissolve(ctx):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    geoms = ctx[0]
    union = unary_union(geoms)
    chunks_done.value += 1
    items_done.value += len(geoms)
    if chunks_done.value % 100 == 0:
        print(f"Dissolved {chunks_done.value} chunks, {float(items_done.value)/total_items.value*100:.2f}%", end="\r")
    return union, ctx[1]

def gpkg_tnx_interface():
    # if _check_pickle("geom_final_partitions"):
    #     return _geom_unary_merge_union([])
    graph = graph_utils.get_graph()
    print(f"Sanitizing")
    wbids = _all_wbids()
    print(f"Got {len(wbids)} wbids")
    # print(f"First 10: {list(wbids)[:10]}")
    # exit()
    graph = graph_utils.generate_subgraph(graph, set([v.index for v in graph.vs if len(v["name"]) > 0]), wbids)
    print(f"Decomposing")
    subgraphs = graph.decompose(minelements=1, mode="weak")
    print(f"Conus-wide decomposition has {len(subgraphs)} subgraphs")

    sgraph_gt_len = lambda s, l: len(s.vs) > l
    subgraphs_ = [s for s in subgraphs if sgraph_gt_len(s, 1)]
    assert len(subgraphs_) > 0
    
    sgraph_has_pref = lambda s, pref: len(s.vs.select(lambda v: pref in v["name"])) > 0
    sgraph_has_tnx = lambda s: sgraph_has_pref(s, "tnx")
    sgraph_has_cnx = lambda s: sgraph_has_pref(s, "cnx")

    # wbids = _all_wbids()
    # print(f"Got {len(wbids)} wbids")

    subgraphs = [s for s in subgraphs if len(s.vs.select(lambda v: v["name"] in wbids)) > 0]
    print(f"Trimmed decomposition to {len(subgraphs)} subgraphs with valid names")

    percent = lambda x, total: f"{x/total*100:.2f}%"

    wb_subset_indices = {}
    if _check_pickle("wb_subset_indices"):
        wb_subset_indices = _get_pickle("wb_subset_indices")
    else:
        num_finished = Value("i", 0) #chunks [processes]
        n_finished = Value("i", 0) #items
        total = Value("i", len(subgraphs))
        with Pool(initializer=_mp_init_shared_mem, initargs=(num_finished, n_finished, total)) as p:
            print(f"Starting pool with {p._processes} processes")
            try:
                pool_result = p.imap_unordered(_mp_create_wbid_map, [(i, subgraph) for i, subgraph in enumerate(subgraphs)], chunksize=10)
                pool_result = list(pool_result)
                n_ = len(subgraphs)
                while n_ > 1:
                    num_finished.value = 0
                    n_finished.value = 0
                    total.value = n_
                    chunk_size = 100
                    chunks = _data_partition(pool_result, chunk_size)
                    pool_result2 = p.imap_unordered(_mp_merge_wbid_maps, [(chunks[i],) for i in range(len(chunks))], chunksize=1)
                    pool_result2 = list(pool_result2)
                    n_ = len(pool_result2)
                    pool_result = pool_result2
                    print(f"Reduced to {n_} chunks. {percent(len(subgraphs) - n_, len(subgraphs))}", end="\r")
                wb_subset_indices = pool_result2[0]
            except KeyboardInterrupt:
                current_count = p._processes
                p.terminate()
                p.join()
                raise KeyboardInterrupt(f"Terminated {current_count} processes")
            print(f"Closing pool")
        _send_pickle(wb_subset_indices, "wb_subset_indices")

    print(f"Initialized wb to subset indices map")
    geoms = gpkg_utils.get_geom_from_wbids_map(wbids)
    print(f"Got {len(geoms)} geometries")

    #check subgraph mapping validity
    for i, v in enumerate(subgraphs):
        for v_ in v.vs:
            if v_["name"] in wb_subset_indices:
                assert wb_subset_indices[v_["name"]] == i

    # sorted_subsets = sorted(subgraphs, key=lambda s: len(s.vs), reverse=True)
    ##
    subset_geoms = []
    sort_order = [(i, len(s.vs)) for i, s in enumerate(subgraphs)]
    sort_order = sorted(sort_order, key=lambda x: x[1], reverse=True)
    sizes = {}
    names = {}
    global PARTITION_THRESHOLD
    last_progress = 0
    last_progress_time = time.time()
    prog_ = lambda x, total: x/total*100
    if _check_pickle(f"geom_final_partitions_{len(subgraphs)}_{len(geoms)}"):
        _savedata = _get_pickle(f"geom_final_partitions_{len(subgraphs)}_{len(geoms)}")
        sizes = _savedata["sizes"]
        subset_geoms = _savedata["subset_geoms"]
    for i, v in sort_order:
        if i in sizes:
            continue
        complete_ = sum([sizes[i_] for i_ in sizes])
        if complete_ > 0:
            if prog_(complete_, len(geoms)) - last_progress > 3 or time.time() - last_progress_time > 10:
                print(f"Completed {prog_(complete_, len(geoms)):.2f}%", end="\n")
                last_progress = prog_(complete_, len(geoms))
                last_progress_time = time.time()
                _savedata = {"sizes": sizes, "subset_geoms": subset_geoms}
                _send_pickle(_savedata, f"geom_final_partitions_{len(subgraphs)}_{len(geoms)}")
            # print(f"Completed {percent(complete_, len(geoms))}", end="\n")
        if v < PARTITION_THRESHOLD:
            if v > 1000 or len(sizes)%10 == 0:
                pass
                # print(f"Basic merge for {i} with {v} vertices")
            geom_ = [geoms[v_["name"]] for v_ in subgraphs[i].vs if v_["name"] in geoms]
            subset_geom = unary_union(geom_)
            subset_geoms.append(subset_geom)
            sizes[i] = len(geom_)
            continue
        global ALLOW_FILE_CACHE, SUBSET_CACHE_THRESHOLD
        prev_val = ALLOW_FILE_CACHE
        ALLOW_FILE_CACHE = (v > SUBSET_CACHE_THRESHOLD) and prev_val
        if ALLOW_FILE_CACHE:
            print(f"Allowing file cache for {i} with {v} vertices")
        before_simplify = time.time()
        geom_ = [geoms[v_["name"]] for v_ in subgraphs[i].vs if v_["name"] in geoms]
        unique_subset_name = f"{i}_{v}_{subgraphs[i].vs[0]['name']}"
        print(f"Adding {unique_subset_name} with {v} vertices and {len(geom_)} geometries")
        subset_geom = _geom_unary_merge_union(geom_, name=unique_subset_name)
        subset_geoms.append(subset_geom)
        sizes[i] = len(geom_)
        names[i] = unique_subset_name
        after_simplify = time.time()
        print(f"Simplified {i} in {after_simplify - before_simplify} seconds")
        ALLOW_FILE_CACHE = prev_val
    print(f"Created {len(subset_geoms)} subset geometries")
    _savedata = {"sizes": sizes, "subset_geoms": subset_geoms}
    _send_pickle(_savedata, f"geom_final_partitions_{len(subgraphs)}_{len(geoms)}")

    subset_geom_order = [(i, v[0]) for i, v in enumerate(sort_order)]
    subset_geom_order = sorted(subset_geom_order, key=lambda x: x[1], reverse=True)
    subset_geoms = [subset_geoms[i] for i, _ in subset_geom_order]
    for i, geom in enumerate(subset_geoms):
        if isinstance(geom, dict):
            assert len(geom) == 1
            firstkey = list(geom.keys())[0]
            subset_geoms[i] = geom[firstkey]
    #resort the sizes to match up with the subset_geoms
    size_list = [sizes[i] for i, _ in subset_geom_order]
    gdf_initial = GeoDataFrame({"geometry": subset_geoms, "size": size_list}, crs="EPSG:5070", geometry="geometry")

    # new_cols = {"name": [], "has_tnx": [], "has_cnx": []}

    get_name = lambda i: names[i] if i in names else f"subset_{i}"
    subgraph_names = [get_name(i) for i in range(len(subgraphs))]
    subgraph_has_tnx = [sgraph_has_tnx(s) for s in subgraphs]
    subgraph_has_cnx = [sgraph_has_cnx(s) for s in subgraphs]
    
    gdf_initial["name"] = subgraph_names
    gdf_initial["has_tnx"] = subgraph_has_tnx
    gdf_initial["has_cnx"] = subgraph_has_cnx

    print(f"Created initial GeoDataFrame")

    filename = f"flow_subgraphs_{len(subgraphs)}_{len(geoms)}.gpkg"

    print(f"Creating and processing {filename}")

    file_start = time.time()
    _create_gpkg(file_paths.root_output_dir() / filename)
    _cleanup_gpkg(file_paths.root_output_dir() / filename)
    _insert_data_gpkg(file_paths.root_output_dir() / filename, gdf_initial, "flow_subgraphs")
    file_end = time.time()

    print(f"Created, cleaned, and inserted data into gpkg in {file_end - file_start} seconds")


def _fix_gpkg(subset_output_dir: str, gpkg_name_out: str, gpkg_name: str) -> None:
    output_gpkg = subset_output_dir / gpkg_name_out
    os.system(f"ogr2ogr -f GPKG {subset_output_dir / gpkg_name} {output_gpkg}")
    os.system(f"rm {output_gpkg}* && mv {subset_output_dir / gpkg_name} {output_gpkg}")

def _create_gpkg(pkg_path: str):
    template = file_paths.template_gpkg()
    os.system(f"cp {template} {pkg_path}")

def _cleanup_gpkg(pkg_path: str):
    gpkg_utils.remove_triggers(pkg_path)

def _insert_data_gpkg(pkg_path: str, data: GeoDataFrame, table: str):
    # dest_db = sqlite3.connect(pkg_path)
    # print(f"Connected to {pkg_path}")
    # #check if table exists
    # query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    # if len(dest_db.execute(query).fetchall()) == 0:
    #     #create table
    #     print(f"Creating table {table}")
    #     columns = list(data.columns)
    #     column_types = [type(data[c][0]) for c in columns]
    #     type_selector = lambda t: {
    #         int: "INTEGER",
    #         float: "REAL",
    #         str: "TEXT",
    #         bytes: "BLOB"
    #     }[t]
    #     column_types = [type_selector(t) for t in column_types]
    #     exec_str = f"CREATE TABLE {table} ({', '.join([f'{columns[i]} {column_types[i]}' for i in range(len(columns))])})"
    #     dest_db.execute(exec_str)
    #     dest_db.commit()
    # #insert data
    # print(f"Inserting data into {table}")
    # columns = list(data.columns)
    # columns_str = ", ".join(columns)
    # values_str = ", ".join(["?" for _ in columns])
    # exec_str = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str})"
    # dest_db.executemany(exec_str, data.values)
    # dest_db.commit()
    # dest_db.close()
    if isinstance(data, dict):
        data = GeoDataFrame(data)
    data.to_file(pkg_path, layer=table, driver="GPKG")
    print(f"Done inserting data into {table}")

def _mp_find_bounds(ctx):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    geoms = ctx[0]
    bounds = geoms[0].bounds
    for g in geoms[1:]:
        bounds = (
            min(bounds[0], g.bounds[0]), 
            min(bounds[1], g.bounds[1]), 
            max(bounds[2], g.bounds[2]), 
            max(bounds[3], g.bounds[3])
            )
    data["min_x"].value = min(bounds[0] - 1, data["min_x"].value)
    data["min_y"].value = min(bounds[1] - 1, data["min_y"].value)
    data["max_x"].value = max(bounds[2] + 1, data["max_x"].value)
    data["max_y"].value = max(bounds[3] + 1, data["max_y"].value)
    data["num_finished"].value += 1
    if data["num_finished"].value % 100 == 0:
        print(f"Processed {data['num_finished'].value}/{data['total'].value} geometries", end="\r")

def _data_partition(data:list, partition_size:int) -> list[list]:
    #Include entire dataset without causing index out of range
    excess = min(len(data) % partition_size, max(len(data) - partition_size, 0))
    last_ind = len(data) // partition_size * partition_size
    # print(f"Data: {len(data)}, Partition: {partition_size}, Excess: {excess}, Last Ind: {last_ind}")
    parts = [data[i:i+partition_size] for i in range(0, len(data), partition_size)]
    if excess > 0:
        parts.append(data[last_ind:])
    # total = sum([len(p) for p in parts])
    # print(f"From {len(data)} to {total} in {len(parts)} parts")
    return parts

def _data_bisect(data: list, bounds: tuple, x=None, y=None, path="")-> list[list, tuple]:
    #Given a line, return two partitions of the data, and their bounds
    #If x is None, use y
    #If y is None, use x
    if len(data) == 0:
        return []
    assert bounds[0] < bounds[2]
    assert bounds[1] < bounds[3]
    centr = lambda g: (lambda b: ((b[0] + b[2]) / 2, (b[1] + b[3]) / 2))(g.bounds)
    if x is None and y is None:
        if bounds[2] - bounds[0] > bounds[3] - bounds[1]:
            x = True
        else:
            y = True
    if y is None:
        data.sort(key=lambda d: d.centroid.x)
        # div = [(i, d) for i, d in enumerate(data) if d.centroid.x < x][-1][0]
        # return [data[:div], data[div:]]
        div = len(data) // 2
        div_x = centr(data[div])[0]
        assert div_x <= bounds[2]
        assert div_x >= bounds[0]
        left_bounds = (bounds[0], bounds[1], div_x, bounds[3])
        assert left_bounds[0] < left_bounds[2]
        assert left_bounds[1] < left_bounds[3]
        right_bounds = (div_x, bounds[1], bounds[2], bounds[3])
        assert right_bounds[0] < right_bounds[2]
        assert right_bounds[1] < right_bounds[3]
        return [
            (data[:div], left_bounds, path + "L"),
            (data[div:], right_bounds, path + "R")
            ]

    else:
        data.sort(key=lambda d: d.centroid.y)
        # div = [(i, d) for i, d in enumerate(data) if d.centroid.y < y][-1][0]
        # return [data[:div], data[div:]]
        div = len(data) // 2
        div_y = centr(data[div])[1]
        try:
            assert div_y <= bounds[3]
            assert div_y >= bounds[1]
        except:
            print(f"Bounds: {bounds}")
            print(f"Div: {div_y}")
            print(f"Data: {len(data)}")
            raise
        lower_bounds = (bounds[0], bounds[1], bounds[2], div_y)
        assert lower_bounds[0] < lower_bounds[2]
        assert lower_bounds[1] < lower_bounds[3]
        upper_bounds = (bounds[0], div_y, bounds[2], bounds[3])
        assert upper_bounds[0] < upper_bounds[2]
        assert upper_bounds[1] < upper_bounds[3]
        return [
            (data[:div], lower_bounds, path + "D"),
            (data[div:], upper_bounds, path + "U")
            ]
    return []


def _geometric_partition(data: list, partition_size: int, bounds: tuple, _depth = 0, path="") -> list[list, tuple]:
    # print(f"{_depth}\x1b[38;2;200;150;150m{len(data)}\x1b[0m", end="", flush=True)
    #Divide data set into spacially contiguous chunks
    #To ignore partitioning of individual geometries, we will just use their centroids
    #Partition_size is the largest number of geometries in a partition we will allow
    #Bounds is the bounding box of the entire dataset
    # print(f"Depth \x1b[38;2;200;150;150m{_depth}\x1b[0m: {len(data)} geometries in partition    ", end="\r")
    if len(data) <= partition_size:
        # print(f"Depth {_depth}: {len(data)} geometries in partition")
        assert bounds[0] < bounds[2]
        assert bounds[1] < bounds[3]
        return [(data, bounds, path)]
    assert bounds[0] < bounds[2]
    assert bounds[1] < bounds[3]
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    parts = []
    if width > height:
        #Divide along x
        x = bounds[0] + width / 2
        parts = _data_bisect(data, bounds, x=x, path=path)
        # part_bounds = [
        #     p[1] for p in parts
        # ]
        # parts = [p[0] for p in parts]
        # _parts = []
        # for i, p in enumerate(parts):
        #     if len(p) > partition_size:
        #         _parts.extend(_geometric_partition(p, partition_size, part_bounds[i], _depth + 1))
        #     else:
        #         _parts.append(p)
        # parts = _parts
    else:
        #Divide along y
        y = bounds[1] + height / 2
        parts = _data_bisect(data, bounds, y=y, path=path)
        # part_bounds = [
        #     p[1] for p in parts
        # ]
        # parts = [p[0] for p in parts]
        # _parts = []
        # for i, p in enumerate(parts):
        #     if len(p) > partition_size:
        #         _parts.extend(_geometric_partition(p, partition_size, part_bounds[i], _depth + 1))
        #     else:
        #         _parts.append(p)
        # parts = _parts
    if _depth < 1:
        _parts = []
        for p, b, path_ in parts:
            if len(p) > partition_size:
                _parts.extend(_geometric_partition(p, partition_size, b, _depth + 1, path = path_))
            else:
                _parts.append((p, b, path_))
        parts = _parts
    # print(f"Depth {_depth}: {len(parts)} partitions")
    return parts

def _mp_partition(ctx):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    geoms = ctx[0]
    partition_size = ctx[1]
    bounds = ctx[2]
    path = ctx[3]
    parts = _geometric_partition(geoms, partition_size, bounds, path=path)
    data["num_finished"].value += 1
    if log2(data["num_finished"].value).is_integer():
        print(f"Partitioned {data['num_finished'].value} chunks, avg size: {sum([len(p) for p, b, path in parts])/len(parts)}", end="\n")
    return parts




def _geom_unary_merge_union(geoms: list, name: str = "") -> Geometry:
    postfix = '_' + name if len(name) > 0 else ''
    if not _check_pickle(f"geom_final_partitions{postfix}"):
        max_x = Value("d", geoms[0].bounds[2])
        min_x = Value("d", geoms[0].bounds[0])
        max_y = Value("d", geoms[0].bounds[3])
        min_y = Value("d", geoms[0].bounds[1])
        data = {"max_x": max_x, "min_x": min_x, "max_y": max_y, "min_y": min_y}
        num_finished = Value("i", 0)
        total = Value("i", len(geoms))
        data.update({"num_finished": num_finished, "total": total})
        if _check_pickle(f"geom_bounds{postfix}"):
            bounds = _get_pickle("geom_bounds" + postfix)
            assert bounds[0] < bounds[2]
            assert bounds[1] < bounds[3]
            print(f"Loaded bounds: {bounds}")
        else:
            # with Pool(initializer=_mp_init_shared_mem_generic, initargs=(data,)) as p:
            #     print(f"Starting pool with {p._processes} processes")
            #     #choose good chunksize
            #     _chunksize = min(100, len(geoms))
            #     _chunksize = max(total.value // (100 * p._processes), _chunksize)
            #     print(f"Using chunksize {_chunksize}")
            #     try:
            #         subchunk_size = 100
            #         subchunks = _data_partition(geoms, subchunk_size)
            #         # we do not care about the results, just the side effects
            #         pool_result = p.imap_unordered(_mp_find_bounds, [(c,) for c in subchunks], chunksize=1)
            #     except KeyboardInterrupt:
            #         current_count = p._processes
            #         p.terminate()
            #         p.join()
            #         raise KeyboardInterrupt(f"Terminated {current_count} processes")
            #     print(f"Closing pool")
            x0 = min([g.bounds[0] for g in geoms])
            y0 = min([g.bounds[1] for g in geoms])
            x1 = max([g.bounds[2] for g in geoms])
            y1 = max([g.bounds[3] for g in geoms])
            bounds = (x0, y0, x1, y1)
            # bounds = (min_x.value, min_y.value, max_x.value, max_y.value)
            assert bounds[0] < bounds[2]
            assert bounds[1] < bounds[3]
            _send_pickle(bounds, "geom_bounds" + postfix)
        print(f"Found bounds: {bounds}")
        desired_chunksize = 10000
        #partition data to accelerate the union process
        min_partitions = 100
        key = "geom_partitions-" + str(min_partitions) + postfix
        if _check_pickle(key):
            partitions = _get_pickle(key)
            print(f"Loaded {len(partitions)} partitions")
        else:
            partitions = _geometric_partition(geoms, desired_chunksize, bounds)
            #partition single-thread until number of partitions is > 20
            for i in range(4):
                if len(partitions) > min_partitions:
                    break
                new_parts = []
                for p, b, path in partitions:
                    if len(p) > desired_chunksize:
                        new_parts.extend(_geometric_partition(p, desired_chunksize, b, path=path))
                    else:
                        new_parts.append((p, b, path))
                partitions = new_parts
            _send_pickle(partitions, key)
        print(f"Partitioned to {len(partitions)} partitions")
        final_partitions = [(p, b, path) for p, b, path in partitions if len(p) <= desired_chunksize]
        partitions = [(p, b, path) for p, b, path in partitions if len(p) > desired_chunksize]
        num_finished.value = 0
        total.value = len(geoms)
        if _check_pickle("geom_final_partitions" + postfix):
            final_partitions = _get_pickle("geom_final_partitions" + postfix)
            print(f"Loaded {len(final_partitions)} final partitions")
        else:
            print(f"Starting pool", flush=True)
            with Pool(initializer=_mp_init_shared_mem_generic, initargs=(data,)) as p:
                print(f"Starting pool with {p._processes} processes 2", flush=True)
                try:
                    _cycle = 0
                    while len(partitions) > 0:
                        # partitions.sort(key=lambda p: len(p[0]), reverse=True)
                        contexts = [(p, desired_chunksize, b, path) for p, b, path in partitions]
                        pool_result = p.imap_unordered(_mp_partition, contexts, chunksize=5)
                        pool_result = list(pool_result)
                        partitions = []
                        for p_ in pool_result:
                            for p, b, path in p_:
                                if len(p) > desired_chunksize:
                                    partitions.append((p, b, path))
                                else:
                                    final_partitions.append((p, b, path))
                        _cycle += 1
                        if len(partitions) > 0:
                            print(f"Cycle {_cycle}: Partitions: {len(partitions)}, MaxSize: {max([len(p) for p, _, _ in partitions])}", end="\n")
                        # print(f"Cycle {_cycle}: Partitions: {len(partitions)}, MaxSize: {max([len(p) for p, _ in partitions])}", end="\n")
                except KeyboardInterrupt:
                    current_count = p._processes
                    p.terminate()
                    p.join()
                    raise KeyboardInterrupt(f"Terminated {current_count} processes")
                print(f"Closing pool")
            _send_pickle(final_partitions, "geom_final_partitions" + postfix)
    else:
        final_partitions = _get_pickle("geom_final_partitions" + postfix)
        bounds = _get_pickle("geom_bounds" + postfix)
    assert bounds is not None
    assert bounds[0] < bounds[2]
    assert bounds[1] < bounds[3]
    total_area = (bounds[2] - bounds[0]) * (bounds[3] - bounds[1])
    print(f"Final partitions: {len(final_partitions)}")
    print(f"Average size: {sum([len(p) for p, _, _ in final_partitions])/len(final_partitions)}")
    print(f"Total area: {total_area}")
    filled_area = sum([(b[2] - b[0]) * (b[3] - b[1]) for _, b, _ in final_partitions])
    print(f"Filled area: {filled_area}")
    diagnostic_needed = False
    if diagnostic_needed:
        fig, ax = plt.subplots()
        #ensure shapes are visible
        f_width = bounds[2] - bounds[0]
        f_height = bounds[3] - bounds[1]
        print(f"Bounds: {bounds}")
        print(f"Width: {f_width}, Height: {f_height}")
        # xmin = bounds[0] - f_width * 0.1
        # xmax = bounds[2] + f_width * 0.1
        # ymin = bounds[1] - f_height * 0.1
        # ymax = bounds[3] + f_height * 0.1
        scale_factor = 1 / max(f_width, f_height)
        scaled_x = lambda x: (x - bounds[0]) / f_width
        scaled_y = lambda y: (y - bounds[1]) / f_height

        for _, b, _ in final_partitions:
            print(f"Adding rectangle: {b}")
            assert b[0] < b[2]
            assert b[1] < b[3]
            xmin, ymin, xmax, ymax = b
            xmin, ymin, xmax, ymax = scaled_x(xmin), scaled_y(ymin), scaled_x(xmax), scaled_y(ymax)
            width = xmax - xmin
            height = ymax - ymin
            # print(f"Adding rectangle: {xmin}, {ymin}, {width}, {height}")
            xoffset = 0
            yoffset = 0
            # xoffset = width * 0.05
            # yoffset = height * 0.05
            # width -= xoffset
            # height -= yoffset
            #fill with random color
            face_color = (random.random(), random.random(), random.random(), 0.2)
            edge_color = (0, 0, 0, 0.5)
            rect = plt.Rectangle((
                xmin + xoffset, ymin + yoffset
            ), width, height, facecolor=face_color, edgecolor=edge_color, alpha=0.5)
            ax.add_patch(rect)
        plt.show()
        plt.savefig(file_paths.root_output_dir() / "final_partitions" + postfix + ".png")
        return
    if _check_pickle("geom_unions_final" + postfix):
        return _get_pickle("geom_unions_final" + postfix)
    needed_unions = len(final_partitions)
    if _check_pickle("geom_unions_checkpoint" + postfix):
        unions = _get_pickle("geom_unions_checkpoint" + postfix)
        print(f"Loaded {len(unions)} unions")
        needed_unions = len(unions)
    else:
        unions = {}
    new_unions = 0
    if needed_unions != len(final_partitions):
        pass
    else:
        for i, (p, b, path) in enumerate(final_partitions):
            if i in unions:
                continue
            print(f"Unioning partition {i}/{len(final_partitions)}")
            unions[i] = unary_union(p)
            new_unions += 1
            if new_unions % 4 == 0 and new_unions > 0:
                print(f"Unioned {new_unions} partitions", end="\n")
                _send_pickle(unions, "geom_unions_checkpoint" + postfix)

    path_to_partition = {}
    for k, (_, _, path) in enumerate(final_partitions):
        steps = list(path)
        loc = path_to_partition
        for i in range(len(steps)):
            step = steps[i]
            if step not in loc:
                if i == len(steps) - 1:
                    loc[step] = k
                else:
                    loc[step] = {}
            loc = loc[step]
    # print(f"Path to partition: {path_to_partition}")
    def _get_next_layer(tree, path="", list_=[], _depth = 0)->list:
        #dfs
        if len(tree) < 1:
            return list_
        for k, v in tree.items():
            if isinstance(v, dict):
                _get_next_layer(v, path + k, list_, _depth + 1)
            else:
                list_.append((path + k, v))
        return list_
    
    def _set_path(tree, path, value):
        steps = list(path)
        loc = tree
        for i in range(len(steps)):
            step = steps[i]
            if i == len(steps) - 1:
                prev = loc[step]
                loc[step] = value
                # print(f"Set path {path} to {value} from {prev}")
                return
            else:
                loc = loc[step]
        if len(loc) < 1:
            del loc
        print(f"Failed to set path {path} to {value}")

    if needed_unions != len(final_partitions):
        original_tree_size = int(log2(len(final_partitions)))
        needed_tree_size = int(log2(needed_unions))
        print(f"Original tree size: {original_tree_size}, Needed tree size: {needed_tree_size}")
        for i in range(original_tree_size - needed_tree_size):
            #Reduce tree size by getting next layer
            #and spoofing the branch truncation
            targets = _get_next_layer(path_to_partition, list_=[])
            pairpaths = [targets[p][0][0:-1] for p in range(0, len(targets), 2)]
            for i, p in enumerate(pairpaths):
                _set_path(path_to_partition, p, i)
        
    chunks_done = Value("i", 0)
    items_done = Value("i", 0)
    total_items = Value("i", len(final_partitions))
    hard_stop = False
    for i in range(len(final_partitions)):
        if len(path_to_partition) <= 1 or hard_stop or len(unions) <= 1:
            break
        targets = _get_next_layer(path_to_partition, list_=[])
        print(f"Targets: {targets}")
        pairs = [(i, i + 1) for i in range(0, len(targets), 2)]
        pairpaths = [targets[p[0]][0][0:-1] for p in pairs]
        # print(f"Pairpaths: {pairpaths}")
        # exit()
        with Pool(initializer=_mp_init_shared_mem, initargs=(chunks_done, items_done, total_items)) as p:
            print(f"Starting pool with {p._processes} processes")
            try:
                print(f"Unioning {pairs}")
                chunks = [[[unions[p[0]], unions[p[1]]], k] for k, p in enumerate(pairs)]
                pool_result = p.imap(_mp_subset_dissolve, chunks, chunksize=1)
                pool_result = list(pool_result)
                for r in pool_result:
                    unions[r[1]] = r[0]
                    # print(f"Unioned {r[1]}, setting path")
                    _set_path(path_to_partition, pairpaths[r[1]], r[1])
                for i in range(len(pairs), len(unions)):
                    # remove old unions
                    # print(f"Removing union {i}")
                    del unions[i]
            except KeyboardInterrupt:
                current_count = p._processes
                p.terminate()
                p.join()
                raise KeyboardInterrupt(f"Terminated {current_count} processes")
            print(f"Closing pool")



    print(f"Unioned {new_unions} partitions")
    assert len(unions) == 1
    _send_pickle(unions, "geom_unions_final" + postfix)
    #export current geometries to gpkg for qgis visualization
    # data_out = {"name": [], "geometry": []}
    # for i, u in unions.items():
    #     data_out["name"].append(f"union_{i}")
    #     data_out["geometry"].append(u)
    # gdf = GeoDataFrame(data, crs="EPSG:5070")
    # _create_gpkg(file_paths.root_output_dir() / "final_partitions.gpkg")
    # _cleanup_gpkg(file_paths.root_output_dir() / "final_partitions.gpkg")
    # _insert_data_gpkg(file_paths.root_output_dir() / "final_partitions.gpkg", data_out, "unions")
    return unions[0]
    
