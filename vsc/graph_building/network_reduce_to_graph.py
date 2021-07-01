#!/usr/bin/env python

import os
import pandas as pd
import networkx as nx
import json
from argparse import ArgumentParser

if __name__ == '__main__':
    arg_parser = ArgumentParser(description='reduce processed Parquet files into nx.MultiDiGraph')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    options = arg_parser.parse_args()

    in_path = '{0}/osm_roads/roads_intersected.parquet/'.format(options.dir)
    out_path = '{0}/osm_roads/roads.json'.format(options.dir)

    # Reduce futures to a NetworkX Graph as they arrive back with results
    G = nx.MultiDiGraph()

    for i, fn in enumerate(os.listdir(in_path)):
        df = pd.read_parquet(in_path+fn)
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

    # Save the Graph as JSON data
    data = nx.node_link_data(G)

    for i in range(len(data['nodes'])):
        data['nodes'][i]['junction'] = [int(junc) for junc in data['nodes'][i]['junction']]

    with open(out_path,'w') as file:
        json.dump(data,file)
