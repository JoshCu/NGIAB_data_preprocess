import logging
import sqlite3
from functools import cache
from pathlib import Path
from typing import List, Union

import igraph as ig

from data_processing.file_paths import file_paths


def create_graph_from_gpkg(hydrofabric: Path) -> ig.Graph:
    """
    Create a directed network flow graph from a geopackage.

    Args:
        hydrofabric (Path): Path to the geopackage.

    Returns:
        ig.Graph: The constructed graph.
    """
    sql_query = "SELECT id , toid FROM network WHERE id IS NOT NULL"
    con = sqlite3.connect(str(hydrofabric.absolute()))
    merged = con.execute(sql_query).fetchall()
    con.close()
    merged = list(set(merged))
    logging.debug("Building Graph Network with igraph")
    network_graph = ig.Graph.TupleList(merged, directed=True)
    return network_graph

@cache
def get_graph() -> ig.Graph:
    """
    Attempts to load a saved graph, if it doesn't exist, creates one.

    Returns:
        ig.Graph: The graph.
    """
    pickled_graph_path = file_paths.hydrofabric_graph()
    network_graph = ig.Graph()
    if not pickled_graph_path.exists():
        # get data needed to construct the graph
        network_graph = create_graph_from_gpkg(file_paths.conus_hydrofabric())
        # save the graph
        network_graph.write_pickle(pickled_graph_path)

    network_graph = network_graph.Read_Pickle(pickled_graph_path)
    logging.debug(network_graph.summary())
    return network_graph


def get_upstream_ids(names: Union[str, List[str]]) -> List[str]:
    """
    Get the ids of all nodes upstream of the given nodes.

    Args:
        names (Union[str, List[str]]): A string of one name or list of names of the nodes.

    Returns:
        List[str]: The upstream IDs.
    """
    graph = get_graph()
    if isinstance(names, str):
        names = [names]
    parent_names = []
    for name in names:
        id = graph.vs.find(name=name).index
        parents = graph.subcomponent(id, mode="in")
        # get names of ids in parents
        parent_names.extend([graph.vs[x]["name"] for x in parents])

    return parent_names

def get_flow_lines_in_set(upstream_ids:list)->dict:
    # if not getattr(get_flow_lines_in_set, "cache"):
    #     get_flow_lines_in_set.cache = {}
    graph = get_graph()
    to_lines = []
    to_wbs = {}
    if not isinstance(upstream_ids, list):
        upstream_ids = [upstream_ids]
    for name in upstream_ids:
        node = graph.vs.find(name=name)
        to_nexi = node.successors()
        for nex in to_nexi:
            nm = nex["name"]
            to_lines.append([name, nm])
            if nm in to_wbs:
                continue
            to_wbs[nm] = []
            for next in nex.successors():
                to_wbs[nm].append(next["name"])
    return {"to_lines":to_lines, "to_wbs":to_wbs}

