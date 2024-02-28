import logging
import sqlite3
from functools import cache
from pathlib import Path
from typing import List, Union

import igraph as ig

from data_processing.file_paths import file_paths


def create_graph_from_gpkg(hydrofabric: Path) -> ig.Graph:
    """
    Creates a directed network flow graph from a geopackage file, representing hydrological connections.

    This function reads edge connections (id to toid) from the specified geopackage, deduplicates them,
    and constructs a directed graph where edges represent water flow directions.

    Args:
        hydrofabric (Path): The file path to the geopackage containing hydrological data.

    Returns:
        ig.Graph: A directed graph representing the hydrological network.
    """
    sql_query = "SELECT id, toid FROM network WHERE id IS NOT NULL"
    try:
        con = sqlite3.connect(str(hydrofabric.absolute()))
        edges = con.execute(sql_query).fetchall()
        con.close()
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        raise

    # Remove duplicate edges for an accurate representation of the network
    unique_edges = list(set(edges))
    logging.debug("Building hydrological graph network with igraph.")
    network_graph = ig.Graph.TupleList(unique_edges, directed=True)
    return network_graph


@cache
def get_graph() -> ig.Graph:
    """
    Attempts to load a graph from a pickled file; if unavailable, creates it from the geopackage.

    This function first checks if a pickled version of the graph exists. If not, it creates a new graph
    by reading hydrological data from a geopackage file and then pickles the newly created graph for future use.

    Returns:
        ig.Graph: The hydrological network graph.
    """
    pickled_graph_path = file_paths.hydrofabric_graph()
    if not pickled_graph_path.exists():
        logging.debug("Graph pickle does not exist, creating a new graph.")
        network_graph = create_graph_from_gpkg(file_paths.conus_hydrofabric())
        network_graph.write_pickle(pickled_graph_path)
    else:
        try:
            network_graph = ig.Graph.Read_Pickle(pickled_graph_path)
        except Exception as e:
            logging.error(f"Error loading graph pickle: {e}")
            raise

    logging.debug(network_graph.summary())
    return network_graph


def get_upstream_ids(names: Union[str, List[str]]) -> List[str]:
    """
    Retrieves IDs of all nodes upstream of the given nodes in the hydrological network.

    Given one or more node names, this function identifies all upstream nodes in the network,
    effectively tracing the water flow back to its source(s).

    Args:
        names (Union[str, List[str]]): A single node name or a list of node names.

    Returns:
        List[str]: A list of IDs for all nodes upstream of the specified node(s).
    """
    graph = get_graph()
    if isinstance(names, str):
        names = [names]
    parent_ids = set()
    for name in names:
        if name in parent_ids:
            continue
        node_index = graph.vs.find(name=name).index
        upstream_nodes = graph.subcomponent(node_index, mode="IN")
        parent_ids.add([graph.vs[node_id]["name"] for node_id in upstream_nodes])

    return parent_ids


def get_flow_lines_in_set(upstream_ids: Union[str, List[str]]) -> dict:
    """
    Retrieves flow lines and water bodies associated with given upstream IDs.
    Args:
        upstream_ids (Union[str, List[str]]): The upstream IDs to process.

    Returns:
        dict: A dictionary containing 'to_lines' and 'to_wbs' keys. 'to_lines' maps to a list of
              flow lines, and 'to_wbs' maps to a dictionary of water bodies.
    """
    graph = get_graph()
    if isinstance(upstream_ids, str):
        upstream_ids = [upstream_ids]

    to_lines = []
    to_wbs = {}
    for name in upstream_ids:
        process_upstream_id(name, graph, to_lines, to_wbs)

    return {"to_lines": to_lines, "to_wbs": to_wbs}


def process_upstream_id(
    name: str, graph: ig.Graph, to_lines: List[List[str]], to_wbs: dict
) -> None:
    """
    Processes a single upstream ID to update the to_lines and to_wbs structures.

    Args:
        name (str): The name of the upstream ID being processed.
        graph (ig.Graph): The graph object.
        to_lines (List[List[str]]): Accumulator for lines in the graph.
        to_wbs (dict): Accumulator for waterbodies in the graph.
    """
    node = graph.vs.find(name=name)
    to_nexi = node.successors()
    for nex in to_nexi:
        nm = nex["name"]
        to_lines.append([name, nm])
        if nm not in to_wbs:
            to_wbs[nm] = [next["name"] for next in nex.successors()]
