import sys, sqlite3
from pathlib import Path
from typing import List, Tuple, Dict, Any, Union
from functools import cache

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


@cache
def all_wbids():
    db = sqlite3.connect(file_paths.conus_hydrofabric())
    data = db.execute("SELECT id FROM divides").fetchall()
    db.close()
    return set([d[0] for d in data if isinstance(d[0], str) and "wb" in d[0]])

def check_wbids_valid(wbids:set)->set:
    geoms = gpkg_utils.get_geom_from_wbids_map(wbids)
    return set([k for k, v in geoms.items() if v is not None])