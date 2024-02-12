import sqlite3
from pathlib import Path
import logging
from typing import List, Union

import igraph as ig
from data_processing.file_paths import file_paths
import random
import pickle
import os
import shapely

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


def get_graph() -> ig.Graph:
    """
    Attempts to load a saved graph, if it doesn't exist, creates one.

    Returns:
        ig.Graph: The graph.
    """
    if hasattr(get_graph, "cached_graph"):
        return get_graph.cached_graph
    pickled_graph_path = file_paths.hydrofabric_graph()
    network_graph = ig.Graph()
    if not pickled_graph_path.exists():
        # get data needed to construct the graph
        network_graph = create_graph_from_gpkg(file_paths.conus_hydrofabric())
        # save the graph
        network_graph.write_pickle(pickled_graph_path)

    network_graph = network_graph.Read_Pickle(pickled_graph_path)
    logging.debug(network_graph.summary())
    get_graph.cached_graph = network_graph
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

def _utility_graph_stats():
    graph = get_graph()
    all_nodes = graph.vs
    nexi = list(filter(lambda x: "nex" in x["name"], all_nodes))
    wbi = list(filter(lambda x: "wb" in x["name"], all_nodes))
    tnxi = list(filter(lambda x: "tnx" in x["name"], all_nodes))
    print(f"all_nodes = {len(all_nodes)}")
    print(f"nexi = {len(nexi)}, {100*len(nexi)/len(all_nodes):.2f}%")
    print(f"wbi = {len(wbi)}, {100*len(wbi)/len(all_nodes):.2f}%")
    print(f"total = {len(nexi)+len(wbi)}, {100*(len(nexi)+len(wbi))/len(all_nodes):.2f}%")
    # nthr = list(filter(lambda x: "nex" not in x["name"] and "wb" not in x["name"], all_nodes))
    # print(f"neither(?) = {len(nthr)}, {100*len(nthr)/len(all_nodes):.2f}%")
    print(f"tnxs = {len(tnxi)}, {100*(len(tnxi))/len(all_nodes):.2f}%")
    # exit()
    # group = random.sample(nthr, 20)
    # print(f"Neither examples:")
    # for x in group:
    #     print(f"[{x}:{[('nameless' if not hasattr(y,'name') else y['name']) for y in list(x.all_edges())]}]")
    # group:list[ig.Vertex]
    # return nthr
    connections = {}
    conn_to = {}
    conn_from = {}
    conn_both = {}
    count = {}
    edgenum = {}
    attrs = {}
    for node in all_nodes:
        prefix = 'wb' if 'wb' in node['name'] else node['name'][:3]
        if not prefix in count:
            count[prefix] = 0
            edgenum[prefix] = 0
            connections[prefix] = {"none":0}
            conn_from[prefix] = {"none":0}
            conn_to[prefix] = {"none":0}
            conn_both[prefix] = {}
        count[prefix] += 1
        cnxi = node.neighbors("in")
        cnxo = node.neighbors("out")
        cnx = cnxi + cnxo
        edgenum[prefix] += len(cnx)
        for attr in node.attribute_names():
            attrs[attr] = attrs.get(attr, 0) + 1
        for cnode in cnx:
            # print(type(cnode))
            # exit()
            # cnode = graph.vs.find(name_ne=None)
            cname = ('nameless' if not cnode['name'] else cnode['name'])
            cprefix = 'N/A' if cname=='nameless' else ('wb' if 'wb' in cnode['name'] else cnode['name'][:3])
            if not cprefix in connections[prefix]:
                connections[prefix][cprefix] = 0
            connections[prefix][cprefix] += 1
            if cnode in cnxi:
                conn_from[prefix][cprefix] = conn_from[prefix].get(cprefix, 0) + 1
            if cnode in cnxo:
                conn_to[prefix][cprefix] = conn_to[prefix].get(cprefix, 0) + 1
            if cnode in cnxo and cnode in cnxi:
                conn_both[prefix][cprefix] = conn_both[prefix].get(cprefix, 0) + 1
        if len(cnxi) < 1:
            conn_from[prefix]["none"] += 1
        if len(cnxo) < 1:
            conn_to[prefix]["none"] += 1
        if len(cnx) < 1:
            connections[prefix]["none"] += 1
    print(f"total conn:{connections}")
    print(f"total count:{count}")
    def str_2d_per(targ):
        return str({x: {y:f"{targ[x][y]/count[x]:.3f}" for y in targ[x]} for x in targ})
    print(f"total connections per: " + str_2d_per(connections))
    print(f"connect out/to: "+str(conn_to))
    print(f"connect out/to per: "+str_2d_per(conn_to))
    print(f"connect in/from: "+str(conn_from))
    print(f"connect in/from per: "+str_2d_per(conn_from))
    print(f"connect in+out/both: "+str(conn_both))
    print(f"connect in+out/both per: "+str_2d_per(conn_both))
    print(edgenum)
    print(attrs)



from shapely.wkb import loads
def local_blob_to_geom(blob):
    # from http://www.geopackage.org/spec/#gpb_format
    # byte 0-2 don't need
    # byte 3 bit 0 (bit 24)= 0 for little endian, 1 for big endian (used for srs id and envelope type)
    # byte 3 bit 1-3 (bit 25-27)= envelope type (needed to calculate envelope size)
    # byte 3 bit 4 (bit 28)= empty geometry flag
    envelope_type = (blob[3] & 14) >> 1
    empty = (blob[3] & 16) >> 4
    if empty:
        return None
    envelope_sizes = [0, 32, 48, 48, 64]
    envelope_size = envelope_sizes[envelope_type]
    header_byte_length = 8 + envelope_size
    # everything after the header is the geometry
    geom = blob[header_byte_length:]
    # convert to hex
    geometry = loads(geom)
    return geometry
import sqlite3

def has_cache(filename):
    return os.path.isfile(file_paths.root_output_dir()/(filename+".pickle"))
def get_cache(filename):
    with open(file_paths.root_output_dir()/(filename+".pickle"),"rb") as f:
        return pickle.load(f)
def send_cache(filename, data):
    try:
        with open(file_paths.root_output_dir()/(filename+".pickle"),"wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        print("Failed!")
        raise e
    finally:
        # print("Success!")
        pass

def _utility_graphdata()->dict[dict]:
    graph = get_graph()
    all_nodes = graph.vs
    
    result = {
        "wb":[],
        "nex":[],
        "tnx":[]
    }
    for node in all_nodes:
        prefix = 'wb' if 'wb' in node['name'] else node['name'][:3]
        name = node['name']
        datanode = {
            "id":name, 
            "name":name,
            "type":prefix,
            "to_list":[],
            "from_list":[],
            "geom": None,
            "geotype": None
            }
        cnxi = node.neighbors("in")
        cnxo = node.neighbors("out")
        for cn in cnxi:
            datanode["from_list"].append(cn["name"])
        for cn in cnxo:
            datanode["to_list"].append(cn["name"])
        result[name] = datanode
        result[prefix].append(name)
    print(__file__.split("/")[-1] + " > " + ", ".join([f"{k}_len:{len(result[k])}" for k in ["wb", "nex", "tnx"]]))
    return result

def handle_geom(graphdata:dict[dict])->dict:
    ids = graphdata["wb"]
    total = len(ids)
    if has_cache("geom_result"):
        result = get_cache("geom_result")
    else:
        # format ids as ('id1', 'id2', 'id3')
        geopackage = file_paths.conus_hydrofabric()
        sql_query = f"SELECT id, geom FROM divides WHERE id IN {tuple(ids)}"
        # remove the trailing comma from single element tuples
        sql_query = sql_query.replace(",)", ")")
        # would be nice to use geopandas here but it doesn't support sql on geopackages
        con = sqlite3.connect(geopackage)
        result = con.execute(sql_query).fetchall()
        con.close()
        send_cache("geom_result", result)
    nexi = graphdata["nex"]
    if has_cache("nexi_result"):
        result1 = get_cache("nexi_result")
    else:
        # format ids as ('id1', 'id2', 'id3')
        geopackage = file_paths.conus_hydrofabric()
        sql_query = f"SELECT id, geom FROM nexus WHERE id IN {tuple(nexi)}"
        # remove the trailing comma from single element tuples
        sql_query = sql_query.replace(",)", ")")
        # would be nice to use geopandas here but it doesn't support sql on geopackages
        con = sqlite3.connect(geopackage)
        result1 = con.execute(sql_query).fetchall()
        con.close()
        send_cache("nexi_result", result1)

    results = 0
    hasgeom = 0
    first = True
    for id, geom in result:
        geometry = local_blob_to_geom(geom)
        if geometry is not None:
            graphdata[id]["geom"] = geometry
            graphdata[id]["geotype"] = type(geometry)
            if first:
                print(graphdata[id])
                first=False
            hasgeom+=1
        results+=1
    print(f"Found: {results}\nHasGeom: {hasgeom}\nTotal: {total}")
    first = True
    for id, geom in result1:
        geometry = local_blob_to_geom(geom)
        if geometry is not None:
            graphdata[id]["geom"] = geometry
            graphdata[id]["geotype"] = type(geometry)
            if first:
                print(graphdata[id])
                first=False
    #To [nex] from [this wb]
    graphdata["to_lines"] = {}
    graphdata["leaving_multiline"] = {}
    #From [nex] to [this wb]
    graphdata["from_lines"] = {}
    graphdata["arriving_multiline"] = {}

    for id in graphdata["wb"]:
        node = graphdata[id]
        if node.get("geotype", None) is None:
            continue
        centroid = node["geom"].centroid
        # if len(node["to_list"])>0:
        graphdata["from_lines"][id] = []

        for nexid in node["to_list"]:
            nex = graphdata[nexid]
            if nex.get("geotype", None) is None:
                continue
            pos = nex["geom"]
            try:    
                line = shapely.LineString([centroid, pos])
            except Exception as e:
                print(type(centroid), centroid, type(pos), pos, nex)
                raise e
            graphdata["from_lines"][id].append(line)
        if len(graphdata["from_lines"][id])>0:
            graphdata["leaving_multiline"][id] = shapely.MultiLineString(graphdata["from_lines"][id])


        # if len(node["from_list"])>0:
        graphdata["to_lines"][id] = []

        for nexid in node["from_list"]:
            nex = graphdata[nexid]
            if not "geotype" in nex:
                continue
            pos = nex["geom"]
            line = shapely.LineString([centroid, pos])
            graphdata["to_lines"][id].append(line)
        if len(graphdata["to_lines"][id])>0:
            graphdata["arriving_multiline"][id] = shapely.MultiLineString(graphdata["to_lines"][id])
        
def get_geom_data()->dict:
    if hasattr(get_geom_data,"cached_data"):
        return get_geom_data.cached_data
    if has_cache("all_graphdata"):
        graphdata = get_cache("all_graphdata")
    else:
        graphdata = _utility_graphdata()
        handle_geom(graphdata)
        send_cache("all_graphdata", graphdata)
    get_geom_data.cached_data = graphdata
    return graphdata

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

