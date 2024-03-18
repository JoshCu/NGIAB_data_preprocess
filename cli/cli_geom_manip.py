## Standard Libraries
import os
import sys
import time
import math
import random
import signal
from pathlib import Path
import pickle
from functools import cache, partial
import multiprocessing as mp
import json

## Graph Manipulation
import igraph as ig

## Data Manipulation
from geopandas import GeoDataFrame
import sqlite3

## Data Visualization
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import matplotlib.patches as mpatches

## Geometry Manipulation
from shapely.geometry import Polygon, MultiPolygon, LineString, MultiLineString, Point, MultiPoint
from shapely.ops import unary_union
from shapely import Geometry

##Intra-package imports
#Facilitate in-file testing
#check if module exists in sys.modules
#if not, add the parent module to the path
if 'data_processing' not in sys.modules:
    #we are in (root)/cli, data_processing is at (root)/data_processing
    sys.path.append(str(Path(__file__).parent.parent))
parent_module = sys.modules['.'.join(__name__.split('.')[:-1]) or '__main__']
if __name__ == '__main__' or parent_module.__name__ == '__main__':
    from data_processing.file_paths import file_paths
    import data_processing.graph_utils as graph_utils
    import data_processing.gpkg_utils as gpkg_utils
    import data_processing.subset as subset
else:
    from data_processing.file_paths import file_paths
    from data_processing import graph_utils
    from data_processing import gpkg_utils
    from data_processing import subset

ONCE = (True,)
RENDER = False


def mp_is_child():
    return mp.current_process().name != 'MainProcess'

def mp_init_shared_mem_generic(_data):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    global data
    data = {}
    data.update(_data)

@cache
def get_shared_vars_mp()->dict:
    return {
        "total_done": mp.Value('i', 0),
        "total_items": mp.Value('i', 0),
        "processes_started": mp.Value('i', 0),
        "processes_finished": mp.Value('i', 0)
    }

def reset_shared_vars_mp(shared_vars:dict):
    with shared_vars["total_done"].get_lock():
        shared_vars["total_done"].value = 0
    with shared_vars["total_items"].get_lock():
        shared_vars["total_items"].value = 0
    with shared_vars["processes_started"].get_lock():
        shared_vars["processes_started"].value = 0
    with shared_vars["processes_finished"].get_lock():
        shared_vars["processes_finished"].value = 0

def mp_process_start(shared_vars:dict):
    with shared_vars["processes_started"].get_lock():
        shared_vars["processes_started"].value += 1

def mp_process_finish(shared_vars:dict):
    with shared_vars["processes_finished"].get_lock():
        shared_vars["processes_finished"].value += 1

def mp_process_done(shared_vars:dict, count:int):
    with shared_vars["total_done"].get_lock():
        shared_vars["total_done"].value += count

def mp_process_items(shared_vars:dict, count:int):
    with shared_vars["total_items"].get_lock():
        shared_vars["total_items"].value += count

@cache
def all_wbids():
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    data = db.execute("SELECT id FROM divides").fetchall()
    db.close()
    return set([d[0] for d in data if isinstance(d[0], str) and "wb" in d[0]])

@cache
def vpu_list()->list:
    #expected return ['01', '02', '03N', '03S', '03W', '04', '05', '06', '07', '08', '09', '10L', '10U', '11', '12', '13', '14', '15', '16', '17', '18']
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    #want unique, non-na values for the vpu field in the network table
    data = db.execute("SELECT DISTINCT vpu FROM network WHERE vpu IS NOT NULL").fetchall()
    db.close()
    return [d[0] for d in data if isinstance(d[0], str) and len(d[0]) > 0]

@cache
def vpu_stats()->dict:
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    #names in the network table take the form "prefix-uuid"
    # where prefix is a 2-3 letter code for the type of entry
    # and uuid is a unique identifier
    # we want to count the number of entries for each prefix
    # select the vpu, the prefix, and the count of the prefix
    # group by the vpu and the prefix
    query1 = "SELECT vpu, substr(id, 1, instr(id, '-') - 1) as prefix, count(*) as count FROM network GROUP BY vpu, prefix"
    data = db.execute(query1).fetchall()
    db.close()
    prefix_counts = {}
    vpu_counts = {}
    vpu_totals = {}
    for vpu, prefix, count in data:
        if vpu not in vpu_counts:
            vpu_counts[vpu] = {}
            vpu_totals[vpu] = 0
        vpu_counts[vpu][prefix] = count
        vpu_totals[vpu] += count
        if prefix not in prefix_counts:
            prefix_counts[prefix] = 0
        prefix_counts[prefix] += count
    return {
        "vpu_counts": vpu_counts,
        "vpu_totals": vpu_totals,
        "prefix_counts": prefix_counts
    }

@cache
def get_vpu_wbids(vpu:str)->set:
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    query = "SELECT id FROM network WHERE vpu = ?"
    data = db.execute(query, (vpu,)).fetchall()
    db.close()
    return set([d[0] for d in data if isinstance(d[0], str) and "wb" in d[0]])

def check_wbids_valid(wbids:set)->set:
    geoms = gpkg_utils.get_geom_from_wbids_map(wbids)
    return set([k for k, v in geoms.items() if v is not None])

def geom_get_bounds(geoms: list)->tuple:
    return (
        min([g.bounds[0] for g in geoms]),
        min([g.bounds[1] for g in geoms]),
        max([g.bounds[2] for g in geoms]),
        max([g.bounds[3] for g in geoms])
    )

def debug_render_partitions(postfix:str, parts:list, bounds:tuple):
    if not RENDER:
        return
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

    for p in parts:
        b = p[1]
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
    plt.savefig(file_paths.root_output_dir() / f"partition_{postfix}.png")

def debug_make_subplot(fig, parts:list, bounds:tuple,)->plt.Axes:
    before_len = len(fig.axes)
    ax = fig.add_subplot(1, before_len + 1, before_len + 1)
    #ensure shapes are visible
    f_width = bounds[2] - bounds[0]
    f_height = bounds[3] - bounds[1]
    scale_factor = 1 / max(f_width, f_height)
    scaled_x = lambda x: (x - bounds[0]) / f_width
    scaled_y = lambda y: (y - bounds[1]) / f_height
    for p in parts:
        b = p[1]
        xmin, ymin, xmax, ymax = b
        xmin, ymin, xmax, ymax = scaled_x(xmin), scaled_y(ymin), scaled_x(xmax), scaled_y(ymax)
        width = xmax - xmin
        height = ymax - ymin
        xoffset = 0
        yoffset = 0
        face_color = (random.random(), random.random(), random.random(), 0.2)
        edge_color = (0, 0, 0, 0.5)
        rect = plt.Rectangle((
            xmin + xoffset, ymin + yoffset
        ), width, height, facecolor=face_color, edgecolor=edge_color, alpha=0.5)
        ax.add_patch(rect)
    return ax
    


def data_bisect(data: list, bounds: tuple, path="")-> list[list, tuple, str, tuple, tuple]:
    #Given a line, return two partitions of the data, and their bounds
    if len(data) == 0:
        return []
    # centr = lambda g: (lambda b: ((b[0] + b[2]) / 2, (b[1] + b[3]) / 2))(g.bounds)
    centr = lambda g: (g.centroid.x, g.centroid.y)
    axis = 0 if bounds[2] - bounds[0] > bounds[3] - bounds[1] else 1
    sortby = lambda x: centr(x)[0] if axis == 0 else centr(x)[1]
    repl = lambda i, ax, v: tuple(v if ((~(k % 2) ^ ax) & (k//2 == i)) else l for k, l in enumerate(bounds))
    dirs = ["L", "R"] if axis == 0 else ["D", "U"]
    data.sort(key=sortby)
    div = len(data) // 2
    div_val = sortby(data[div])
    bound0 = repl(1, axis, div_val)
    bound1 = repl(0, axis, div_val)
    return [
        (data[:div], bound0, path + dirs[0], bounds, centr(data[div])),
        (data[div:], bound1, path + dirs[1], bounds, centr(data[div]))
        ]

def geometric_partition(
        data: list, 
        partition_size: int, 
        bounds: tuple,
        path=""
        ) -> list[list, tuple, str, tuple, tuple]:
    assert bounds[0] < bounds[2], f"Bounds are not valid: {bounds}, {bounds[0]} < {bounds[2]}"
    assert bounds[1] < bounds[3], f"Bounds are not valid: {bounds}, {bounds[1]} < {bounds[3]}"
    if len(data) <= partition_size:
        return [(data, bounds, path)]
    parts = data_bisect(data, bounds, path)
    # print(f"From {bounds} to {parts[0][1]} and {parts[1][1]}")
    return parts

#reference based version of data_bisect
# ideally, saves memory by not copying the data, and only manipulating
# the list of wbids, not the geometries
def data_bisect_reference(data:list[str], geoms:dict, bounds:tuple, path="")->list[list, tuple, str, tuple, tuple]:
    if len(data) == 0:
        return []
    centr = lambda g: (g.centroid.x, g.centroid.y)
    axis = 0 if bounds[2] - bounds[0] > bounds[3] - bounds[1] else 1
    sortby = lambda x: centr(geoms[x])[0] if axis == 0 else centr(geoms[x])[1]
    repl = lambda i, ax, v: tuple(v if ((~(k % 2) ^ ax) & (k//2 == i)) else l for k, l in enumerate(bounds))
    dirs = ["L", "R"] if axis == 0 else ["D", "U"]
    data.sort(key=sortby)
    div = len(data) // 2
    div_val = sortby(data[div])
    bound0 = repl(1, axis, div_val)
    bound1 = repl(0, axis, div_val)
    return [
        (data[:div], bound0, path + dirs[0], bounds, centr(geoms[data[div]])),
        (data[div:], bound1, path + dirs[1], bounds, centr(geoms[data[div]]))
        ]

#reference based version of geometric_partition
# same purpose as data_bisect_reference,
# ideally saves memory by not copying the data
def geometric_partition_reference(
        data: list,
        geoms: dict,
        partition_size: int,
        bounds: tuple,
        path=""
        ) -> list[list, tuple, str, tuple, tuple]:
    assert bounds[0] < bounds[2], f"Bounds are not valid: {bounds}, {bounds[0]} < {bounds[2]}"
    assert bounds[1] < bounds[3], f"Bounds are not valid: {bounds}, {bounds[1]} < {bounds[3]}"
    if len(data) <= partition_size:
        return [(data, bounds, path)]
    parts = data_bisect_reference(data, geoms, bounds, path)
    return parts

def mp_collect_partitions(args:dict)->list[list, tuple, str, tuple, tuple]:
    is_child = mp_is_child()
    if is_child:
        mp_process_start(data)
    part0 = args["part0"]
    part1 = args["part1"]
    total_data = part0[0] + part1[0]
    total_bounds = (
        min(part0[1][0], part1[1][0]),
        min(part0[1][1], part1[1][1]),
        max(part0[1][2], part1[1][2]),
        max(part0[1][3], part1[1][3])
    )
    path_len = len(part0[2])
    assert path_len == len(part1[2]), f"Paths are not the same length: {part0[2]} and {part1[2]}"
    path_len = max(path_len - 1, 0)
    assert part0[2][:path_len] == part1[2][:path_len], f"Paths are not the same: {part0[2]} and {part1[2]}"
    merge_geom = unary_union(total_data)
    if is_child:
        mp_process_done(data, 1)
        mp_process_items(data, len(total_data))
        mp_process_finish(data)
    return [([merge_geom], total_bounds, part0[2][:path_len])]
    
def collect_step_mp(parts)->list[list, tuple, str]:
    start_len = len(parts)
    if start_len == 1:
        return parts
    try:
        parts.sort(key=lambda x: x[2])
    except Exception as e:
        print("Error in sort")
        print(len(parts))
        print([len(p) for p in parts])
        print(parts)
        print(f"Parts: {[(len(p[0]), p[2]) for p in parts]}")
        raise e
    args = [{"part0": parts[i], "part1": parts[i+1]} for i in range(0, len(parts), 2)]
    try:
        initdata = get_shared_vars_mp()
        with mp.Pool(mp.cpu_count(), mp_init_shared_mem_generic, (initdata,)) as pool:
            results = pool.map(mp_collect_partitions, args)
        results = [r for rs in results for r in rs]
        final_len = len(results)
        assert final_len < start_len, f"Final length is not less than start length: {final_len} >= {start_len}"
        return results
    except KeyboardInterrupt:
        print("Keyboard interrupt")
        raise KeyboardInterrupt
    except Exception as e:
        print("Error in pool.map")
        print(len(parts))
        print([len(p) for p in parts])
        print(parts)
        print(f"Parts: {[(len(p[0]), p[2]) for p in parts]}")
        raise e
    
def mp_partition_subdivide(args:dict)->list[list, tuple, str, tuple, tuple]:
    is_child = mp_is_child()
    if is_child:
        mp_process_start(data)
    part0 = args["part0"]
    total_data = part0[0]
    total_bounds = part0[1]
    path = part0[2]
    partition_size = args["partition_size"]
    if len(total_data) > partition_size:
        parts = geometric_partition(total_data, partition_size, total_bounds, path)
    else:
        parts = [part0]
    if is_child:
        mp_process_done(data, len(parts))
        mp_process_items(data, len(total_data))
        mp_process_finish(data)
    return parts
    
def partition_step_mp(parts:list, partition_size:int)->list[list, tuple, str]:
    start_len = len(parts)
    if all(len(p[0]) <= partition_size for p in parts):
        return parts
    if start_len < 4:
        results = [mp_partition_subdivide({"part0": p, "partition_size": partition_size}) for p in parts]
        results = [r for rs in results for r in rs]
        final_len = len(results)
        assert final_len > start_len, f"Final length is not greater than start length: {final_len} <= {start_len}"
        return results
    try:
        initdata = get_shared_vars_mp()
        args = [{"part0": p, "partition_size": partition_size} for p in parts]
        with mp.Pool(mp.cpu_count(), mp_init_shared_mem_generic, (initdata,)) as pool:
            results = pool.map(mp_partition_subdivide, args)
        #results are a list of pairs, need to flatten
        results = [r for rs in results for r in rs]
        final_len = len(results)
        assert final_len > start_len, f"Final length is not greater than start length: {final_len} <= {start_len}"
        return results
    except KeyboardInterrupt:
        print("Keyboard interrupt")
        raise KeyboardInterrupt
    except Exception as e:
        print("Error in pool.map")
        raise e

    




def heavy_union(
        geoms:list,
        partition_size:int = 5000,
        mp_threshold:int = 10000
        )->Geometry:
    do_mp = len(geoms) > mp_threshold and not mp_is_child()
    if do_mp:
        parts = geometric_partition(geoms, partition_size, geom_get_bounds(geoms))
        while not all(len(p[0]) <= partition_size for p in parts):
            _parts = partition_step_mp(parts, partition_size)
            parts = _parts
        if RENDER:
            bounds = geom_get_bounds(geoms)
            _i = 0
        while len(parts) > 1:
            if RENDER:
                debug_render_partitions(_i, parts, bounds)
                _i += 1
            _parts = collect_step_mp(parts)
            parts = _parts
        return parts[0][0]
    else:
        return unary_union(geoms)

        

if __name__ == '__main__':
    do_partition = False
    if do_partition:
        all_wbs = all_wbids()
        geoms_dict = gpkg_utils.get_geom_from_wbids_map(all_wbs)
        geoms = list(geoms_dict.values())
        getbounds = lambda gs: (
            min([g.bounds[0] for g in gs]),
            min([g.bounds[1] for g in gs]),
            max([g.bounds[2] for g in gs]),
            max([g.bounds[3] for g in gs])
        )
        bounds = getbounds(geoms)
        print(bounds, len(geoms))
        _desired = 15000
        parts = geometric_partition(geoms, _desired, bounds)
        for _ in range(10):
            _parts = []
            for k, p in enumerate(parts):
                try:
                    _parts.extend(geometric_partition(p[0], _desired, p[1], p[2]))
                except Exception as e:
                    print(p[1:], k)
                    print(getbounds(p[0]))
                    # print(e)
                    raise e
                # _parts.extend(geometric_partition(p[0], _desired, p[1], p[2]))
            parts = _parts
            if all(len(p[0]) <= _desired for p in _parts):
                break
        print(len(parts))
    
    do_vpu_list = True
    if do_vpu_list:
        print(vpu_list())
        stats = vpu_stats()
        print(json.dumps(stats, indent=4))
        #get the smallest vpu, ignoring "None"
        smallest = min([k for k in stats['vpu_totals'] if k is not None], key=lambda k: stats['vpu_totals'][k])
        print(smallest, stats['vpu_totals'][smallest])
        vpu_wbids = get_vpu_wbids(smallest)
        print(f"Got {len(vpu_wbids)} wbids for {smallest}")
        valid_wbids = check_wbids_valid(vpu_wbids)
        print(f"Got {len(valid_wbids)} valid wbids for {smallest}")
        start_time = time.time()
        geoms_dict = gpkg_utils.get_geom_from_wbids_map(valid_wbids)
        print(f"Got {len(geoms_dict)} geometries in {time.time() - start_time} seconds")
        start_time = time.time()
        merged_geom = heavy_union([geoms_dict[w] for w in valid_wbids])
        print(f"Union took {time.time() - start_time} seconds")



    