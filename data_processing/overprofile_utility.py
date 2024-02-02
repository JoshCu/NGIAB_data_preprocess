import cProfile, pstats
import data_processing.subset as subset
import data_processing.graph_utils as graph_utils
import data_processing.gpkg_utils as gpkg_utils
import data_processing.forcings as forcings
from data_processing.file_paths import file_paths

import sqlite3
import random
#File created to start working towards a 
# standard semi-automated performance test
# If this file can test each function category
# extensively, significant performance improvements
# should be measurable.

runs = 0
class ModuleMeasurement:
    def __init__(self, name:str):
        self.name = name
        self.profiler = cProfile.Profile()
        self.callstats = {}

    def stat(self, key:str, val:int|None=None)->int:
        if key not in self.callstats:
            self.callstats[key] = 0
        if val is None:
            return self.callstats[key]
        else:
            self.callstats[key] = val
            return self.callstats[key]
        
    def inc(self, key:str)->None:
        self.stat(key, self.stat(key) + 1)

    def __enter__(self, *args):
        self.inc("enter")
        self.profiler.enable()
        
    def __exit__(self, *args):
        self.profiler.disable()
        self.inc("exit")
        self.print()

    def print(self):
        with open(file_paths.root_output_dir() / "_profiling" / (self.name + ".txt"), "w") as f:
            f.write(repr(self.callstats)+"\n")
            stat = pstats.Stats(self.profiler, stream=f)
            stat.sort_stats("cumtime")
            stat.print_stats()
            stat.dump_stats(file_paths.root_output_dir() / "_profiling" / (self.name+".prof"))

def heavy_benchmark(append:str="")->None:
    global runs
    if runs>0:
        return
    runs+=1
    print("called")
    modules = [
        #Actual modules:
        "subset", "graph_utils", "gpkg_utils", "forcings", "forcings", 
        "subset_deterministic",
        #Setting up for tests here:
        "setup"
        ]
    categories = {x:ModuleMeasurement(x+append) for x in modules}
    with categories["setup"]:
        random.seed(0)

        #excessive subset test
        geopackage = file_paths.conus_hydrofabric()
        sql_query = f"SELECT id FROM divides"
        con = sqlite3.connect(geopackage)
        all_ids = con.execute(sql_query).fetchall()
        con.close()
        all_ids = [x[0] for x in all_ids]
        # wb_only_lmb = lambda tup: "nex" not in tup[0]
        # wb_only = filter(wb_only_lmb, all_ids)
        # random_filter = lambda tup: random.random()<0.5 and tup is not None
        # random_subset = lambda largeset: list(filter(random_filter,largeset))
        # random_subsets = [random_subset(all_ids) for _ in range(10)]
        sanitize = lambda x: x is not None
        clean_ids = list(filter(sanitize,all_ids))
        random_subsets = [[random.choice(clean_ids) for _ in range(10)] for _ in range(10)]

        type_dict = {}
        for rset in random_subsets:
            for item in rset:
                typestr = str(type(item))
                if typestr not in type_dict:
                    type_dict[typestr]=0
                type_dict[typestr]+=1
        print(type_dict)
    
    with categories["subset"]:
        for rand_set in random_subsets:
            subset.subset(rand_set)
            print("Did subset!")

    deterministic_set = ["wb-1319934"]
    with categories["subset_deterministic"]:
        for det_set in deterministic_set:
            subset.subset(det_set)
            print("Did subset!")

    stat = pstats.Stats(categories["subset"].profiler)
    stat.sort_stats("cumtime")
    stat.print_stats(20)
    exit()

