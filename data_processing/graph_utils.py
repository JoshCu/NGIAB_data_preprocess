import logging
import sqlite3
from functools import cache
from pathlib import Path
from typing import List, Union

import igraph as ig

from data_processing.file_paths import file_paths

logger = logging.getLogger(__name__)


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
        logger.error(f"SQLite error: {e}")
        raise

    # Remove duplicate edges for an accurate representation of the network
    unique_edges = list(set(edges))
    logger.debug("Building hydrological graph network with igraph.")
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
        for node in upstream_nodes:
            parent_ids.add(graph.vs[node]["name"])

    return list(parent_ids)


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

def wbids_groupby_component(ids: list[str]):# -> list[set[str]]:
    graph = get_graph()
    verts = [graph.vs.find(name=id) for id in ids]
    print(f"{len(ids)} ids -> {len(verts)} vertices")
    vset = set([v.index for v in verts])
    for v in verts:
        successors = v.successors()
        for s in successors:
            vset.add(s.index)
    print(f"Subgraph has {len(vset)} vertices")
    subgraph = generate_subgraph(graph, vset)
    subgraphs = subgraph.decompose(minelements=1)
    print(f"Subgraph has {len(subgraphs)} subgraphs")
    # sgraph_has_tnx = lambda s: len(s.vs.select(lambda v: "tnx" in v["name"] and v["name"]!="tnx-1000006467")) > 0
    sgraph_has_tnx = lambda s: len(s.vs.select(lambda v: "tnx" in v["name"])) > 0
    tnx_subgraphs = [s for s in subgraphs if sgraph_has_tnx(s)]
    # ntnx_subgraphs = [s for s in subgraphs if not sgraph_has_tnx(s)]
    # print(f"Subgraph has {len(tnx_subgraphs)} subgraphs with tnx")
    # print(f"Subgraph has {len(ntnx_subgraphs)} subgraphs without tnx")
    print("Filtering")
    subgraphs_named = [[v["name"] for v in s.vs] for s in subgraphs]
    tnx_subgraphs_named = [[v["name"] for v in s.vs] for s in tnx_subgraphs]
    print(f"TNX Subgraphs number: {len(tnx_subgraphs_named)}")
    # print(f"Subgraphs number: {len(subgraphs_named)}")
    return tnx_subgraphs_named
    # return subgraphs_named

def generate_subgraph(graph: ig.Graph, vset: set[int], wbids:set) -> ig.Graph:
    graph_hash = str(len(graph.vs)) + str(len(graph.es)) + str(id(graph))
    if hasattr(generate_subgraph, "cache") and graph_hash in generate_subgraph.cache:
        return generate_subgraph.cache[graph_hash]
    total = len(vset)
    percent = lambda x: f"{x/total*100:.2f}%"
    new_graph = ig.Graph(directed=True)
    # vnames = {v: graph.vs[v]["name"] for v in vset}
    # vnew_ids = {v: new_graph.add_vertex(name=vnames[v]) for v in vset}
    # vtouched = {v: False for v in vset}
    is_nex_priority = lambda n: "tnx" in n["name"] or "cnx" in n["name"]
    important_nex = {}
    verts = [v for v in graph.vs if v["name"] in wbids]
    print(f"Subgraph has {len(verts)} vertices")
    vnew_ids = {v.index: new_graph.add_vertex(name=v["name"]) for v in verts}
    print(f"Added vertices: {len(vnew_ids)}")
    edges = []
    for v in verts:
        nexi = v.successors()
        
        next_verts = set()
        for n in nexi:
            next_wbs = n.successors()
            next_verts.update(next_wbs)
            if is_nex_priority(n):
                if not n.index in important_nex:
                    important_nex[n.index] = new_graph.add_vertex(name=n["name"])
                edges.append((vnew_ids[v.index], important_nex[n.index]))
        # print(f"Vertex {v['name']} has {len(next_verts)} successors: {next_verts}, {tuple(n['name'] for n in next_verts)}")
        # print(f"Next verts are in wbids: {list((n['name'] in wbids) for n in next_verts)}")
        # print(f"Next verts are in graph: {list((n.index in vnew_ids) for n in next_verts)}")
        # exit()
        for nv in next_verts:
            if nv["name"] not in wbids:
                continue
            try:
                edges.append((vnew_ids[v.index], vnew_ids[nv.index]))
            except KeyError:
                print(f"KeyError: {v.index} -> {nv.index}. {nv['name']}")
                raise

    new_graph.add_edges(edges)
    original_verts = len(graph.vs)
    original_edges = len(graph.es)
    final_verts = len(new_graph.vs)
    final_edges = len(new_graph.es)
    assert final_edges > 0
    print(f"Original: {original_verts} vertices, {original_edges} edges")
    print(f"Final: {final_verts} vertices, {final_edges} edges")
    # for i, v in enumerate(vset):
        
        #/succ = [s for s in graph.vs[v].successors() if s.index in vnames]
        # pred = [s for s in graph.vs[v].predecessors() if s.index in vnames]
        #/edges.extend([(vnew_ids[v], vnew_ids[s.index]) for s in succ])
        # edges.extend([(vnew_ids[v], vnew_ids[s.index]) for s in graph.vs[v].successors() if s.index in vnames])
        # edges.extend([(vnew_ids[s.index], vnew_ids[v]) for s in graph.vs[v].predecessors() if s.index in vnames and not vtouched[s.index]])
        # print(f"Adding edges: i:{percent(i)}, len: {len(edges)}", end="\r")#. S({len(new_graph.subcomponent(vnew_ids[v], 'all'))})",end="\r")
        # vtouched[v] = True
        # edges.extend([(vnew_ids[v], vnew_ids[s.index]) for s in graph.vs[v].successors() if s.index in vnames])
    # new_graph.add_edges(edges)
    # print(f"Added edges: {percent(total)}")
    # print(f"Vertices touched: {sum(vtouched.values())}/{total}")
    if not hasattr(generate_subgraph, "cache"):
        generate_subgraph.cache = {}
    generate_subgraph.cache[graph_hash] = new_graph
    return new_graph
    