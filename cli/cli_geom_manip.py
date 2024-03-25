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

# unary_union = coverage_union_all



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
    from cli.cli_wbid_utils import *
    from cli.cli_vpus import *
    from cli.cli_debug_utils import MemoryMonitor
    from cli.cli_debug_utils import debug_geoplot_geom
    from cli.cli_multiprocess import *
else:
    from data_processing.file_paths import file_paths
    from data_processing import graph_utils
    from data_processing import gpkg_utils
    from data_processing import subset
    from cli_wbid_utils import *
    from cli_vpus import *
    MemoryMonitor = int
    debug_geoplot_geom = lambda *args, **kwargs: None
    from cli_multiprocess import *


ONCE = (True,)
RENDER = False
WORKERMANAGER = None


def mp_is_child():
    return False
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



def geom_get_bounds(geoms: list)->tuple:
    return (
        min([g.bounds[0] for g in geoms]),
        min([g.bounds[1] for g in geoms]),
        max([g.bounds[2] for g in geoms]),
        max([g.bounds[3] for g in geoms])
    )



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

def debug_harvest_parts(partition_tree)->list:
    if isinstance(partition_tree, list):
        result = []
        for p in partition_tree:
            result.extend(debug_harvest_parts(p))
        return result
    return [partition_tree]

def path_tree_decompose(partition_tree:list)->dict:
    parts = debug_harvest_parts(partition_tree)
    sizes = {}
    for p in parts:
        path = p[2]
        longth = len(path)
        if longth not in sizes:
            sizes[longth] = []
        sizes[longth].append(p)
    return sizes


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
    # if not "override" in args:
    #     assert path_len == len(part1[2]), f"Paths are not the same length: {part0[2]} and {part1[2]}"
    path_len = max(path_len - 1, 0)
    # if not "override" in args:
    #     assert part0[2][:path_len] == part1[2][:path_len], f"Paths are not the same: {part0[2]} and {part1[2]}"
    try:
        merge_geom = unary_union(total_data)
    except TypeError as e:
        print("Error in unary_union")
        print(f"Total data type: {type(total_data)}")
        if isinstance(total_data, list):
            print(f"Total data item types: {set([str(type(d)) for d in total_data])}")
            if isinstance(total_data[0], tuple):
                print(f"Total data item 0 types: {set([str(type(d)) for d in total_data[0]])}")
                print(f"Item 0: {total_data[0]}")
        print(f"Parts types: {set([str(type(p)) for p in total_data])}")
        raise e
    if is_child:
        mp_process_done(data, 1)
        mp_process_items(data, len(total_data))
        mp_process_finish(data)
    path_final = part0[2][:path_len] if not "override" in args else part0[2]
    return [([merge_geom], total_bounds, path_final)]
    
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
        # print(parts)
        # print(f"Parts: {[(len(p[0]), p[2]) for p in parts]}")
        raise e

    args = [{"part0": parts[i], "part1": parts[i+1]} for i in range(0, len(parts) - start_len%2, 2)]
    skips = [parts[-1]] if len(parts) % 2 == 1 else []


    print(f"Beginning multiprocessing with {len(args)} pairs")
    try:
        initdata = get_shared_vars_mp()
        # with mp.Pool(mp.cpu_count(), mp_init_shared_mem_generic, (initdata,)) as pool:
        #     results = pool.map(mp_collect_partitions, args)
        results = []
        for i in range(0, len(args), mp.cpu_count()):
            WORKERMANAGER.reset()
            for j in range(mp.cpu_count()):
                k = i + j
                if k >= len(args):
                    break
                arg = args[k]
                WORKERMANAGER.send_task(mp_collect_partitions, arg)
            WORKERMANAGER.distribute()
            results.extend(WORKERMANAGER.task_return.values())
        results = [r for rs in results for r in rs]
        if len(skips) > 0 and len(results) > 0:
            final = results[-1]
            results = results[:-1]
            merged = mp_collect_partitions({"part0": final, "part1": skips[0], "override": True})
            results.extend(merged)
        elif len(skips) > 0:
            results.extend(skips)
        # results.extend(skips)
        final_len = len(results)
        assert final_len < start_len, f"Final length is not less than start length: {final_len} >= {start_len}"
        return results
    except KeyboardInterrupt:
        print("Keyboard interrupt")
        raise KeyboardInterrupt
    except Exception as e:
        print("Error in pool.map")
        print(len(parts))
        # print([len(p) for p in parts])
        # print(parts)
        # print(f"Parts: {[(len(p[0]), p[2]) for p in parts]}")
        raise e

def mp_full_partition_1(geoms:list, partition_size:int, bounds:tuple, profile_dir:Path):
    #alternate method, avoid calling centroid on geometries more than once
    print("Beginning full partition with method 1")
    # profile0 = cProfile.Profile()
    # profile0.enable()
    for g in geoms:
        x, y = g.centroid.x, g.centroid.y
        x0, y0, x1, y1 = g.bounds
        assert x >= x0 and x <= x1, f"Centroid x not in bounds: {x} not in {x0} to {x1}"
        assert y >= y0 and y <= y1, f"Centroid y not in bounds: {y} not in {y0} to {y1}"
        assert x0 < x1, f"Bounds are not valid: {x0} < {x1}"
        assert y0 < y1, f"Bounds are not valid: {y0} < {y1}"
        assert bounds[0] <= x0, f"Bounds are not valid: {bounds[0]} <= {x0}"
        assert bounds[1] <= y0, f"Bounds are not valid: {bounds[1]} <= {y0}"
        assert bounds[2] >= x1, f"Bounds are not valid: {bounds[2]} >= {x1}"
        assert bounds[3] >= y1, f"Bounds are not valid: {bounds[3]} >= {y1}"
    cxs = [(g.centroid.x, g.centroid.y, i) for i, g in enumerate(geoms)]
    total_return = 0
    def recursor(quad, _bounds, path, depth=0):
        nonlocal total_return, partition_size
        if depth > 100:
            print(f"Partition size: {partition_size}")
            print(f"Quad size: {len(quad)}")
            bounds_of_quad = geom_get_bounds([geoms[cx[2]] for cx in quad])
            print(f"Bounds: {bounds_of_quad}")
            raise RecursionError("Too deep")
        if len(quad) <= partition_size:
            total_return += 1
            return [([geoms[cx[2]] for cx in quad], _bounds, path)]
        midp = ((_bounds[0] + _bounds[2]) / 2, (_bounds[1] + _bounds[3]) / 2)
        quads = [[[], []], [[], []]]

        bounds_list = [
            (_bounds[0], midp[1], midp[0], _bounds[3]), #LU
            (_bounds[0], _bounds[1], midp[0], midp[1]), #LD
            (midp[0], midp[1], _bounds[2], _bounds[3]), #RU
            (midp[0], _bounds[1], _bounds[2], midp[1]) #RD
        ]
        in_bnds = lambda cx, b: b[0] <= cx[0] <= b[2] and b[1] <= cx[1] <= b[3]
        for i, cx in enumerate(quad):
            if in_bnds(cx, bounds_list[0]):
                quads[0][0].append(cx)
            elif in_bnds(cx, bounds_list[1]):
                quads[0][1].append(cx)
            elif in_bnds(cx, bounds_list[2]):
                quads[1][0].append(cx)
            elif in_bnds(cx, bounds_list[3]):
                quads[1][1].append(cx)
            else:
                print(f"Error in bounds: {cx} not in {bounds_list}")
                raise RecursionError("Error in recursor")
        lens = [[len(q) for q in quad] for quad in quads]

        paths = [path + dirs for dirs in ["LU", "LD", "RU", "RD"]]
        result = []
        for i, subquad in enumerate(quads):
            rs_ = []
            for j, _quad in enumerate(subquad):
                # assert len(_quad) < len(quad), f"Subquad is not smaller: {len(_quad)} >= {len(quad)}"
                if len(_quad) == 0:
                    continue
                if len(_quad) <= partition_size:
                    rs_.append(([geoms[cx[2]] for cx in _quad], bounds_list[2*i + j], paths[2*i + j]))
                if len(_quad) >= len(quad):
                    total_return += 1
                    try:
                        return recursor(_quad, bounds_list[2*i + j], paths[2*i + j], depth + 1)
                    except RecursionError as e:
                        # if "Error in recursor" in str(e):
                            #we already know the error
                            # raise e
                        print(f"Error in recursor at depth {depth}")
                        print(f"Bounds: {bounds_list}")
                        print(lens)
                        e.args += (f"Error in recursor at depth {depth}",)
                        raise Exception(e)
                rs_.append(recursor(_quad, bounds_list[2*i + j], paths[2*i + j], depth + 1))
            result.append(rs_)
        return result
    parts = recursor(cxs, bounds, "")
    return parts

def mp_full_partition_2(geoms:list, partition_size:int, bounds:tuple, profile_dir:Path):
    #Previous method works by spatial partitioning, this one will return to the original method
    # of partitioning by number of items
    print("Beginning full partition with method 2")
    cxs = [(g.centroid.x, g.centroid.y, i) for i, g in enumerate(geoms)]
    def recursor(cxs, partition_size, bounds, path, depth=0):
        if len(cxs) <= partition_size:
            return [(cxs, bounds, path)]
        mid = len(cxs) // 2
        cxs.sort(key=lambda x: x[0])
        midp = (cxs[mid][0], cxs[mid][1])
        cxs_left = cxs[:mid]
        cxs_right = cxs[mid:]
        bounds_left = (bounds[0], bounds[1], midp[0], bounds[3])
        bounds_right = (midp[0], bounds[1], bounds[2], bounds[3])
        cxs_left.sort(key=lambda x: x[1])
        mid = len(cxs_left) // 2
        midp = (cxs_left[mid][0], cxs_left[mid][1])
        cxs_left_down = cxs_left[:mid]
        cxs_left_up = cxs_left[mid:]
        bounds_left_down = (bounds_left[0], bounds[1], midp[0], midp[1])
        bounds_left_up = (bounds_left[0], midp[1], midp[0], bounds[3])
        cxs_right.sort(key=lambda x: x[1])
        mid = len(cxs_right) // 2
        midp = (cxs_right[mid][0], cxs_right[mid][1])
        cxs_right_down = cxs_right[:mid]
        cxs_right_up = cxs_right[mid:]
        bounds_right_down = (bounds_right[0], bounds[1], midp[0], midp[1])
        bounds_right_up = (bounds_right[0], midp[1], midp[0], bounds[3])
        paths = ["LD", "LU", "RD", "RU"]
        quads = [
            (cxs_left_down, bounds_left_down, path + "LD"),
            (cxs_left_up, bounds_left_up, path + "LU"),
            (cxs_right_down, bounds_right_down, path + "RD"),
            (cxs_right_up, bounds_right_up, path + "RU")
        ]
        result = []
        for i, quad in enumerate(quads):
            rs = recursor(quad[0], partition_size, quad[1], quad[2], depth + 1)
            result.extend(rs)
        return result
    parts = recursor(cxs, partition_size, bounds, "")
    for i, part in enumerate(parts):
        parts[i] = ([geoms[cx[2]] for cx in part[0]], part[1], part[2])

    return parts






FULL_PARTITION_METHOD = mp_full_partition_2




def heavy_union(
        geoms:list,
        partition_size:int = 5000,
        mp_threshold:int = 10000
        )->Geometry:
    do_mp = len(geoms) > mp_threshold and not mp_is_child()
    if not os.path.exists(file_paths.root_output_dir() / "heavy_union"):
        os.makedirs(file_paths.root_output_dir() / "heavy_union")
    profile_dir = file_paths.root_output_dir() / "heavy_union"
    if do_mp:
        bound_time = time.time()
        bounds = geom_get_bounds(geoms)
        for g in geoms:
            x, y = g.centroid.x, g.centroid.y
            x0, y0, x1, y1 = g.bounds
            assert x >= x0 and x <= x1, f"Centroid x not in bounds: {x} not in {x0} to {x1}"
            assert y >= y0 and y <= y1, f"Centroid y not in bounds: {y} not in {y0} to {y1}"
            assert x0 < x1, f"Bounds are not valid: {x0} < {x1}"
            assert y0 < y1, f"Bounds are not valid: {y0} < {y1}"
            assert bounds[0] <= x0, f"Bounds are not valid: {bounds[0]} <= {x0}"
            assert bounds[1] <= y0, f"Bounds are not valid: {bounds[1]} <= {y0}"
            assert bounds[2] >= x1, f"Bounds are not valid: {bounds[2]} >= {x1}"
            assert bounds[3] >= y1, f"Bounds are not valid: {bounds[3]} >= {y1}"
        print(f"Got bounds in {time.time() - bound_time} seconds")
        part_time = time.time()
        part_tree = FULL_PARTITION_METHOD(geoms, partition_size, bounds, profile_dir)
        # parts = debug_harvest_parts(part_tree)
        sizes = path_tree_decompose(part_tree)
        num_parts = sum([len(v) for v in sizes.values()])
        longest = max(sizes.keys())
        parts = sizes[longest]
        sizes.pop(longest)
        print(f"Got {len(parts)} partitions in {time.time() - part_time} seconds")
        # print(f"Got {len(parts)} partitions in group {longest}")
        # print(f"Lens: {[len(p[0]) for p in parts]}")
        _i = len(parts)
        _j = 0
        # print(f"Collecting {len(parts)} partitions")
        collect_time = time.time()
        while len(parts) > 1:
            step_time = time.time()
            _parts = collect_step_mp(parts)
            pathlen = len(_parts[0][2])
            if pathlen not in sizes:
                sizes[pathlen] = []
            # print(f"Got {len(_parts)} partitions to add to group {pathlen}")
            sizes[pathlen].extend(_parts)
            lens_dict = {k: len(v) for k, v in sizes.items()}
            # print(f"Sizes lens now: {lens_dict}")
            longest = max(sizes.keys())
            parts = sizes[longest]
            sizes.pop(longest)
            _i = len(parts)
            # print(f"Got {len(parts)} partitions in group {longest}")
            _j += 1
            print(f"Group {_j} took {time.time()-step_time} seconds")
        print(f"Collected {num_parts} partitions in {time.time() - collect_time} seconds")
        print(f"Finalizing")
        return parts[0][0]
    else:
        return unary_union(geoms)

WORKERMANAGER = WorkerManager("Primary", file_paths.root_output_dir())
WORKERMANAGER.start()

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
    
    do_vpu_list = False
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
        merged_geom = heavy_union([geoms_dict[w] for w in valid_wbids], partition_size=500)
        print(f"Union took {time.time() - start_time} seconds")

    do_all_wbids = False
    if do_all_wbids:
        with MemoryMonitor() as mem:
            start_time = time.time()
            all_wbs = all_wbids()
            print(f"Got {len(all_wbs)} wbids in {time.time() - start_time} seconds")
            start_time = time.time()
            geoms_dict = gpkg_utils.get_geom_from_wbids_map(all_wbs)
            print(f"Got {len(geoms_dict)} geometries in {time.time() - start_time} seconds")
            start_time = time.time()
            merged_geom = heavy_union([geoms_dict[w] for w in all_wbs], partition_size=500)
            print(f"Union took {time.time() - start_time} seconds")

    do_less_than_all_wbids = True
    if do_less_than_all_wbids:
        with MemoryMonitor() as mem:
            start_time = time.time()
            all_wbs = all_wbids()
            print(f"Got {len(all_wbs)} wbids in {time.time() - start_time} seconds")
            start_time = time.time()
            vpus = vpu_list()
            mid = min(len(vpus), 3 * len(vpus) // 2)#, 1)
            half = vpus[:mid]
            half_wbid_groups = [get_vpu_wbids(v) for v in half]
            half_wbids = [wb for wbgroup in half_wbid_groups for wb in wbgroup if wb in all_wbs]
            print(f"Got {len(half_wbids)} wbids in {time.time() - start_time} seconds")
            start_time = time.time()
            geoms_dict = gpkg_utils.get_geom_from_wbids_map(half_wbids)
            print(f"Got {len(geoms_dict)} geometries in {time.time() - start_time} seconds")
            start_time = time.time()
            merged_geom = heavy_union([geoms_dict[w] for w in half_wbids], partition_size=500)
            print(f"Union took {time.time() - start_time} seconds")
            debug_geoplot_geom(merged_geom, bounds=geom_get_bounds(merged_geom))




    