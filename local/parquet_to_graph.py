import os
import pandas as pd
import networkx as nx

# Reduce futures to a NetworkX Graph as they arrive back with results
G = nx.MultiDiGraph()

path = './../data/osm_roads/roads_intersected.parquet/'

for i, fn in enumerate(os.listdir(path)):
    df = pd.read_parquet(path+fn)
    print(f'{i}: Reducing {fn} into the Graph')
    for row in df.itertuples():
        if len(row.nodes) and len(row.edges):
            edges = [(tuple(edge['0']), tuple(edge['1']), {'length':edge['2']['length'],'weight':edge['2']['weight'],'flatness':edge['2']['flatness'],'road_class':row.code,'osm_id':row.Index}) for edge in row.edges]
            G.add_edges_from(edges) # Creates nodes if they do not exist, creates an edge with attributes between them
            for node in row.nodes: # Loops over affected new or existing nodes
                if tuple(node['0']) in G.nodes:
                    if 'junction' not in G.nodes[tuple(node['0'])]:
                        G.nodes[tuple(node['0'])]['junction'] = []
                    if 'altitude' not in G.nodes[tuple(node['0'])]:
                        G.nodes[tuple(node['0'])]['altitude'] = node['1']['altitude']
                    # Updates their attributes with 'osm_id' of streets causing intersection
                    [G.nodes[tuple(node['0'])]['junction'].append(osm_id) for osm_id in node['1']['junction'] if osm_id not in G.nodes[tuple(node['0'])]['junction']]
                else:
                    G.add_nodes_from([(tuple(node['0']), {'altitude':node['1']['altitude'], 'junction':list(node['1']['junction'])})])

nx.write_gpickle(G, './../data/osm_roads/roads.gpickle')