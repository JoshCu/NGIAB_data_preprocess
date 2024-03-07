import logging
import sqlite3
from functools import cache
from pathlib import Path
from typing import List, Union, Set

import igraph as ig

from data_processing.file_paths import file_paths

logger = logging.getLogger(__name__)


def get_from_to_id_pairs(
    hydrofabric: Path = file_paths.conus_hydrofabric(), ids: Set = None
) -> List[tuple]:
    """
    Retrieves the from and to IDs from the specified hydrofabric.

    This function reads the from and to IDs from the specified hydrofabric and returns them as a list of tuples.

    Args:
        hydrofabric (Path, optional): The file path to the hydrofabric. Defaults to file_paths.conus_hydrofabric().
        ids (Set, optional): A set of IDs to filter the results. Defaults to None.
    Returns:
        List[tuple]: A list of tuples containing the from and to IDs.
    """
    sql_query = "SELECT id, toid FROM network WHERE id IS NOT NULL"
    if ids:
        ids = [f"'{x}'" for x in ids]
        sql_query = f"{sql_query} AND id IN ({','.join(ids)})"
    try:
        con = sqlite3.connect(str(hydrofabric.absolute()))
        edges = con.execute(sql_query).fetchall()
        con.close()
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise
    unique_edges = list(set(edges))
    return unique_edges


def create_graph_from_gpkg(hydrofabric: Path) -> ig.Graph:
    """
    Creates a graph from the specified hydrofabric.

    This function reads the hydrological data from the specified geopackage file and creates a graph from it.

    Args:
        hydrofabric (Path): The file path to the hydrofabric.

    Returns:
        ig.Graph: The hydrological network graph.
    """
    edges = get_from_to_id_pairs(hydrofabric)
    graph = ig.Graph.TupleList(edges, directed=True)
    return graph


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
        logger.debug("Graph pickle does not exist, creating a new graph.")
        network_graph = create_graph_from_gpkg(file_paths.conus_hydrofabric())
        network_graph.write_pickle(pickled_graph_path)
    else:
        try:
            network_graph = ig.Graph.Read_Pickle(pickled_graph_path)
        except Exception as e:
            logger.error(f"Error loading graph pickle: {e}")
            raise

    logger.debug(network_graph.summary())
    return network_graph


def get_upstream_ids(names: Union[str, List[str]]) -> Set[str]:
    """
    Retrieves IDs of all nodes upstream of the given nodes in the hydrological network.

    Given one or more node names, this function identifies all upstream nodes in the network,
    effectively tracing the water flow back to its source(s).

    Args:
        names (Union[str, List[str]]): A single node name or a list of node names.

    Returns:
        Set[str]: A list of IDs for all nodes upstream of the specified node(s).
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
        for node in upstream_nodes:
            parent_ids.add(graph.vs[node]["name"])

    return parent_ids
