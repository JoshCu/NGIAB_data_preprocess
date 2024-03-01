import os
import sys
from datetime import datetime

import data_processing.create_realization as realization
import data_processing.file_paths as file_paths
import data_processing.forcings as forcings
import data_processing.gpkg_utils as gpkg_utils
import data_processing.graph_utils as graph_utils
import data_processing.subset as subset
import matplotlib.pyplot as plt
import igraph as ig
# File for methods related to handling the command line interface

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
    with open(file_paths.file_paths.root_output_dir() / "ntnx_subset.txt", "w") as f:
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
    new_graph = ig.Graph()
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
    


    

    