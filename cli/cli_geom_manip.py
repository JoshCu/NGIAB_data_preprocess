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

## Profiling
import tracemalloc
import linecache
from resource import getrusage, RUSAGE_SELF, RUSAGE_CHILDREN
from datetime import datetime
from queue import Queue, Empty
import psutil
import cProfile

def tracemalloc_peek(snapshot, key_type='lineno', limit=3):
    top_stats = snapshot.statistics(key_type)
    print(f"Top {limit} lines")
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        print(f"#{index}: {frame.filename}:{frame.lineno} {stat.size/1024:.1f} KiB")
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print(f"    {line}")
    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print(f"{len(other)} other: {size/1024:.1f} KiB")
    total = sum(stat.size for stat in top_stats)
    print(f"Total allocated size: {total/1024:.1f} KiB")


#Child process to monitor the memory usage of both the main process and its children
def memory_monitor(command_queue: Queue, poll_interval=1, min_interval=10):
    r = "\x1b[0m"
    c_ = lambda r,g,b: f"\x1b[38;2;{r};{g};{b}m"
    memstr = lambda s: f"{c_(155, 155, 0)}{s}{r}"
    mprint = lambda *s: print(*(memstr(x) for x in s if isinstance(x, str)))
    monitor_start = time.time()
    parent = mp.parent_process()
    if parent is None:
        mprint("Memory monitor is not a child process")
        return
    ps_parent = psutil.Process(parent.pid)
    monitor_procs = [ps_parent]

    usage_stats = {} #pid -> (rss, vms)
    usage_stats[parent.pid] = (0, 0)
    def update_usage_stats(proc:psutil.Process, maxs:tuple)->tuple:
        try:
            mem_info = proc.memory_info()
            return (max(mem_info.rss, maxs[0]), max(mem_info.vms, maxs[1]))
        except psutil.NoSuchProcess:
            raise Exception("Process no longer exists")
    def update_usage_stats_all(procs:list, stats:dict):
        for i, p in enumerate(procs):
            try:
                stats[p.pid] = update_usage_stats(p, stats[p.pid])
            except Exception as e:
                if "no longer exists" in str(e):
                    mprint(f"Process {p.pid} no longer exists")
                    del procs[i]
                else:
                    mprint(f"Error updating stats for {p.pid}: {e}")
                    raise e
    def get_summary_stats(stats:dict)->tuple:
        rss = sum([v[0] for v in stats.values()])
        vms = sum([v[1] for v in stats.values()])
        return rss, vms
    def update_children(proc:psutil.Process, procs:list, stats:dict):
        try:
            children = proc.children()
            for c in children:
                if c.pid not in stats:
                    procs.append(c)
                    stats[c.pid] = (0, 0)
        except psutil.NoSuchProcess:
            raise Exception("Process no longer exists")
        
    last_max = (0, 0)
    is_gt = lambda x, y: x[0] > y[0] or x[1] > y[1]
    last_print = time.time()
    snapshot = None
    # tracemalloc.start()
    while True:
        try:
            command = command_queue.get(timeout=poll_interval)
            if command == "STOP":
                mprint("Stopping memory monitor")
                break
        except Empty:
            if not ps_parent.is_running():
                mprint("Parent process is no longer running")
                break
            # Update usage
            update_children(ps_parent, monitor_procs, usage_stats)
            update_usage_stats_all(monitor_procs, usage_stats)
            # Check if usage has increased
            _new_max = get_summary_stats(usage_stats)
            cond_gt = is_gt(_new_max, last_max)
            cond_time = time.time() - last_print > min_interval
            if cond_gt:
                last_max = _new_max
                # snapshot = tracemalloc.take_snapshot()
            if cond_gt or cond_time:
                rss, vms = last_max
                mprint(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                mprint(f"Memory usage: RSS: {rss/1024/1024:.2f} MB, VMS: {vms/1024/1024:.2f} MB")
                mprint(f"Duration: {time.time() - monitor_start:.2f} seconds")
                # if snapshot is not None:
                #     tracemalloc_peek(snapshot)
                last_print = time.time()
    # tracemalloc.stop()
    mprint("Memory monitor stopped")
    mprint(f"Duration: {time.time() - monitor_start:.2f} seconds")
    mprint(f"Full stats: {usage_stats}")
    for p in monitor_procs:
        if parent.pid == p.pid:
            continue
        try:
            p.terminate()
        except Exception as e:
            mprint(f"Error terminating process {p.pid}: {e}")
    

            


        



class MemoryMonitor:
    def __init__(self, poll_interval=1):
        self.poll_interval = poll_interval
        self.command_queue = mp.Queue()
        self.process = mp.Process(target=memory_monitor, args=(self.command_queue, self.poll_interval))
        self.process.start()

    def stop(self):
        print("Stopping memory monitor")
        self.command_queue.put("STOP")
        self.process.join()

    def __enter__(self):
        print("Starting memory monitor")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

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
else:
    from data_processing.file_paths import file_paths
    from data_processing import graph_utils
    from data_processing import gpkg_utils
    from data_processing import subset
    from cli_wbid_utils import *
    from cli_vpus import *


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
    # print(f"Bounds: {bounds}")
    # print(f"Width: {f_width}, Height: {f_height}")
    # xmin = bounds[0] - f_width * 0.1
    # xmax = bounds[2] + f_width * 0.1
    # ymin = bounds[1] - f_height * 0.1
    # ymax = bounds[3] + f_height * 0.1
    scale_factor = 1 / max(f_width, f_height)
    scaled_x = lambda x: (x - bounds[0]) / f_width
    scaled_y = lambda y: (y - bounds[1]) / f_height

    for p in parts:
        b = p[1]
        # print(f"Adding rectangle: {b}")
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

def debug_create_fig_grid(num:int)->tuple:
    if not RENDER:
        return None, None
    cols = num
    rows = 1
    if num > 4 and 4**2 >= num:
        #Fill out the 4x4 grid
        cols = 4
        rows = math.ceil(num / 4)
    elif num > 4**2:
        #Try to keep roughly square
        cols = math.ceil(math.sqrt(num))
        rows = math.ceil(num / cols)
    fig, axs = plt.subplots(rows, cols)
    return fig, axs

def debug_render_subplot(axs, ind, parts:list, bounds:tuple, postfix:str=""):
    if postfix=="":
        postfix = f"_{ind}"
    if not RENDER:
        return
    ax = axs[ind]
    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_aspect('equal', adjustable='box')
    for p in parts:
        b = p[1]
        assert b[0] < b[2]
        assert b[1] < b[3]
        face_color = (random.random(), random.random(), random.random(), 0.2)
        edge_color = (0, 0, 0, 0.5)
        rect = plt.Rectangle(b[:2], b[2] - b[0], b[3] - b[1], facecolor=face_color, edgecolor=edge_color, alpha=0.5)
        ax.add_patch(rect)
    ax.set_title(postfix)
    return ax

def debug_process_and_show_fig(fig, axs, postfix:str):
    if not RENDER:
        return
    for ax in axs:
        ax.axis('off')
    plt.savefig(file_paths.root_output_dir() / f"partition_{postfix}.png")
    # plt.show()
    


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


#good enough version of geometric_partition
# spacially partition, rather than by number of items
# tetrasects the space, rather than bisecting the data
def geometric_partition_spatial(
        data: list,
        partition_size: int,
        bounds: tuple,
        path=""
        ) -> list[list, tuple, str, tuple, tuple]:
    assert bounds[0] < bounds[2], f"Bounds are not valid: {bounds}, {bounds[0]} < {bounds[2]}"
    assert bounds[1] < bounds[3], f"Bounds are not valid: {bounds}, {bounds[1]} < {bounds[3]}"
    if len(data) <= partition_size:
        return [(data, bounds, path)]
    cxs = [(g.centroid.x, g.centroid.y, i) for i, g in enumerate(data)]
    midp = ((bounds[0] + bounds[2]) / 2, (bounds[1] + bounds[3]) / 2)
    left = []
    right = []
    for i in range(len(data)):
        cx = cxs.pop()
        if cx[0] < midp[0]:
            left.append(cx)
        else:
            right.append(cx)
    quads = [[], [], [], []] #LU, LD, RU, RD
    for i, dir_list in enumerate([left, right]):
        for k in range(len(dir_list)):
            _, y, j = dir_list.pop()
            b_dir = 2 * i
            if y < midp[1]:
                quads[b_dir].append(data[j])
            else:
                quads[b_dir + 1].append(data[j])
    bounds_list = [
        (bounds[0], midp[1], midp[0], bounds[3]),
        (bounds[0], bounds[1], midp[0], midp[1]),
        (midp[0], midp[1], bounds[2], bounds[3]),
        (midp[0], bounds[1], bounds[2], midp[1])
    ]
    return [
        (quads[i], bounds_list[i], path + ["LU", "LD", "RU", "RD"][i], bounds, midp)
        for i in range(4)
    ]

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

def path_tree_recompose(sizes:dict)->list:
    parts = [p for k in sizes.values() for p in k]
    parts.sort(key=lambda x: x[2])
    part_tree = []
    part_map = {}
    def place(part, path, tree:list, _map:dict):
        if len(path) == 0:
            tree.append(part)
            _map[path] = part
            return
        if path[0] not in _map:
            _map[path[0]] = {}
            tree.append([path[0], []])
        tree_next_ind = -1
        for i, t in enumerate(tree):
            if t[0] == path[0]:
                tree_next_ind = i
                break
        place(part, path[1:], tree[tree_next_ind][1], _map[path[0]])
    for p in parts:
        place(p, p[2], part_tree, part_map)
    return part_tree
        





PARTITION_METHOD = geometric_partition_spatial

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
    # args = [{"part0": parts[i], "part1": parts[i+1]} for i in range(0, len(parts), 2)]
    # path_length_dict = {}
    # for i, p in enumerate(parts):
    #     path = p[2]
    #     if len(path) not in path_length_dict:
    #         path_length_dict[len(path)] = []
    #     path_length_dict[len(path)].append(i)
    #Sort path dict such that alphabetical order is preserved
    # sortby = lambda x: parts[x][2]
    # for k in path_length_dict.keys():
    #     path_length_dict[k].sort(key=sortby)
    # args = [] #Pair up partitions with the same path length
    # skips = []
    # for path_len, indices in path_length_dict.items():
    #     num = len(indices)
    #     for i in range(0, num if num % 2 == 0 else num - 1, 2):
    #         p1 = parts[indices[i]]
    #         p2 = parts[indices[i+1]]
    #         #must be same up to second to last element
    #         if p1[2][:path_len - 1] != p2[2][:path_len - 1]:
    #             # print(f"Paths are not the same: {p1[2]} and {p2[2]}")
    #             skips.append(p1)
    #             skips.append(p2)
    #             continue
    #         args.append({"part0": p1, "part1": p2})
    #     if num % 2 == 1:
    #         skips.append(parts[indices[-1]])
    # if len(args) == 0:
    #     print("No args")
    #     print(len(parts))
    #     # print([len(p) for p in parts])
    #     # print(parts)
    #     # print(f"Parts: {[(len(p[0]), p[2]) for p in parts]}")
    #     raise Exception("No args")

    #unfold part tree
    # if len(parts) == 2 and isinstance(parts[0], list):
    #     parts = debug_harvest_parts(parts)

    args = [{"part0": parts[i], "part1": parts[i+1]} for i in range(0, len(parts) - start_len%2, 2)]
    skips = [parts[-1]] if len(parts) % 2 == 1 else []


    print(f"Beginning multiprocessing with {len(args)} pairs")
    try:
        initdata = get_shared_vars_mp()
        with mp.Pool(mp.cpu_count(), mp_init_shared_mem_generic, (initdata,)) as pool:
            results = pool.map(mp_collect_partitions, args)
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
        parts = PARTITION_METHOD(total_data, partition_size, total_bounds, path)
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
    if start_len < 4 or True:
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

    

def mp_full_partition_0(geoms:list, partition_size:int, bounds:tuple, profile_dir:Path):
    print("Beginning full partition with method 0")
    profile0 = cProfile.Profile()
    profile0.enable()
    parts = PARTITION_METHOD(geoms, partition_size, bounds)
    profile0.disable()
    profile0.dump_stats(profile_dir / "heavy_union_0.prof")
    _i = 2
    profile1 = cProfile.Profile()
    profile1.enable()
    while not all(len(p[0]) <= partition_size for p in parts):
        _parts = partition_step_mp(parts, partition_size)
        parts = _parts
        _i *= 2
    profile1.disable()
    profile1.dump_stats(profile_dir / "heavy_union_1.prof")
    print(f"Collecting {len(parts)} partitions")
    return parts

def mp_full_partition_1(geoms:list, partition_size:int, bounds:tuple, profile_dir:Path):
    #alternate method, avoid calling centroid on geometries more than once
    print("Beginning full partition with method 1")
    profile0 = cProfile.Profile()
    profile0.enable()
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
    # midp = ((bounds[0] + bounds[2]) / 2, (bounds[1] + bounds[3]) / 2)
    # quads = [[], [], [], []] #LU, LD, RU, RD
    # quad0_time = time.time()
    # quads[0] = [cx for cx in cxs if cx[0] < midp[0] and cx[1] > midp[1]] #LU
    # print(f"Quad 0 took {time.time() - quad0_time} seconds")
    # quads[1] = [cx for cx in cxs if cx[0] < midp[0] and cx[1] < midp[1]] #LD
    # quads[2] = [cx for cx in cxs if cx[0] > midp[0] and cx[1] > midp[1]] #RU
    # quads[3] = [cx for cx in cxs if cx[0] > midp[0] and cx[1] < midp[1]] #RD
    # del cxs
    # bounds_list = [
    #     (bounds[0], midp[1], midp[0], bounds[3]),
    #     (bounds[0], bounds[1], midp[0], midp[1]),
    #     (midp[0], midp[1], bounds[2], bounds[3]),
    #     (midp[0], bounds[1], bounds[2], midp[1])
    # ]
    # paths = ["LU", "LD", "RU", "RD"]
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
        # do_prof = depth!=0 and depth % 3 == 0 and len(quad) > partition_size * 5
        # if do_prof:
        #     profile = cProfile.Profile()
        #     profile.enable()
        #     ptime = time.time()
            # print(f"Profiling {path} at depth {depth} and time {datetime.now()}")
        midp = ((_bounds[0] + _bounds[2]) / 2, (_bounds[1] + _bounds[3]) / 2)
        quads = [[[], []], [[], []]]
        # quads[0][0] = [cx for cx in quad if cx[0] <= midp[0] and cx[1] > midp[1]] #LU
        # quads[0][1] = [cx for cx in quad if cx[0] <= midp[0] and cx[1] <= midp[1]] #LD
        # quads[1][0] = [cx for cx in quad if cx[0] > midp[0] and cx[1] > midp[1]] #RU
        # quads[1][1] = [cx for cx in quad if cx[0] > midp[0] and cx[1] <= midp[1]] #RD
        # lens = [len(q) for q in quads]
        # del quad
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
        # if do_prof:
        #     profile.disable()
        #     if time.time() - ptime > 1:
        #         profile.dump_stats(profile_dir / f"heavy_union_{path}_{depth}.prof")
        #         print(f"Profiling {path} at depth {depth} and time {datetime.now()} took {time.time() - ptime} seconds")
            # profile.dump_stats(profile_dir / f"heavy_union_{path}_{depth}.prof")
        return result
    parts = recursor(cxs, bounds, "")
    profile0.disable()
    profile0.dump_stats(profile_dir / "heavy_union_0.prof")
    print(f"Collecting {len(parts)} partitions")
    print(f"Total return: {total_return}")
    return parts



FULL_PARTITION_METHOD = mp_full_partition_1




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
        part_tree = FULL_PARTITION_METHOD(geoms, partition_size, bounds, profile_dir)
        # parts = debug_harvest_parts(part_tree)
        sizes = path_tree_decompose(part_tree)
        longest = max(sizes.keys())
        parts = sizes[longest]
        sizes.pop(longest)
        print(f"Got {len(parts)} partitions in group {longest}")
        # print(f"Lens: {[len(p[0]) for p in parts]}")
        _i = len(parts)
        if RENDER:
            # bounds = geom_get_bounds(geoms)
            _num = int(math.log2(_i))
            fig, axs = debug_create_fig_grid(_num)
            axs = axs.flatten()
        _j = 0
        # print(f"Collecting {len(parts)} partitions")
        while len(parts) > 1:
            if RENDER:
                bnds = [(0, p[1]) for p in parts]
                debug_render_subplot(axs, _j, bnds, bounds, f"step_{_j}")
                debug_render_partitions(f"step_{_j}", parts, bounds)
            _parts = collect_step_mp(parts)
            pathlen = len(_parts[0][2])
            if pathlen not in sizes:
                sizes[pathlen] = []
            print(f"Got {len(_parts)} partitions to add to group {pathlen}")
            sizes[pathlen].extend(_parts)
            lens_dict = {k: len(v) for k, v in sizes.items()}
            print(f"Sizes lens now: {lens_dict}")
            longest = max(sizes.keys())
            parts = sizes[longest]
            sizes.pop(longest)
            _i = len(parts)
            print(f"Got {len(parts)} partitions in group {longest}")
            _j += 1
        print(f"Finalizing")
        if RENDER:
            debug_process_and_show_fig(fig, axs, "final")
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
            mid = len(vpus)
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




    