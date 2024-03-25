import sys, sqlite3
from pathlib import Path
from typing import List, Tuple, Dict, Any, Union
from functools import cache
import multiprocessing as mp

#Plotting
import matplotlib.pyplot as plt
import random
import math
import geopandas as gpd
import geoplot as gplt
import shapely as shp
from shapely.geometry import Polygon, MultiPolygon

## Profiling
import tracemalloc
import linecache
from resource import getrusage, RUSAGE_SELF, RUSAGE_CHILDREN
from datetime import datetime
from queue import Queue, Empty
import psutil
import cProfile
import time

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

def debug_geoplot_geom(geom, bounds:tuple=None):
    geoms = []
    if isinstance(geom, list):
        for g in geom:
            if isinstance(g, MultiPolygon):
                geoms.extend(g.geoms)
            else:
                geoms.append(g)
    else:
        geoms = [geom]
    _geoms = []
    for g in geoms:
        if isinstance(g, MultiPolygon):
            _geoms.extend(g.geoms)
        else:
            _geoms.append(g)
    geoms = _geoms
    data = {
        "geometry": geoms,
        "dummy": [1 for _ in geoms]
    }
    df = gpd.GeoDataFrame(data, geometry="geometry", crs="EPSG:5070")
    df.to_crs("EPSG:4326", inplace=True)
    bounds = df.total_bounds
    # print(f"Bounds: {bounds}")
    # print(f"Geoms: {geoms}")
    ax = gplt.polyplot(
        df,
        edgecolor='black',
        linewidth=1,
        facecolor='red',
        figsize=(10, 10),
        extent=bounds
        )
    output_path = file_paths.root_output_dir() / "plots" / "geom_plot.png"
    #verify the output path exists
    for p in output_path.parents:
        if not p.exists():
            p.mkdir()
    plt.savefig(output_path)
    
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
def memory_monitor(command_queue: Queue, poll_interval=1, min_interval=10, verbose=False):
    if not verbose:
        #Why monitor memory if you're not going to print it?
        return
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
    #Use as a context manager, create a child process to monitor memory usage
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

UUID = 0
class CProfileMonitor:
    #Use as a context manager, use the cProfile module to profile the code
    def __init__(self, name=None):
        global UUID
        if name is None:
            name = f"profiling_{UUID}"
            UUID += 1
        self.name = name
        self.output_path = file_paths.root_output_dir() / "cProfile" / f"{name}.prof"
        for p in self.output_path.parents:
            if not p.exists():
                p.mkdir()
        self.profile = cProfile.Profile()
        self.profile.enable()

    def stop(self):
        self.profile.disable()
        self.profile.dump_stats(self.output_path)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
