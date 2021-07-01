#!/usr/bin/env python

from argparse import ArgumentParser
from dask.distributed import Client, wait
import networkx as nx
import json
import pandas as pd
import sys
import os


def process(fn_in, fn_out, fn, graph_url):
    with open(graph_url,'r') as file:
        data = json.load(file)
    
    G = nx.node_link_graph(data)

    df = pd.read_parquet(fn_in, columns=['timestamp','snapped_coordinate','graph_node'], engine='pyarrow')
    data = {'timestamp':[], 'x':[], 'y':[]} # Placeholder for interpolated data
    step = 10/3600 # Time step duration in hours (10s)
    i = 0

    while i < (len(df) - 1):
        start = df.iloc[i]
        i += 1 # Increment index to point to next raw sample (increments inside while-loop if next sample is unrealistic)

        # Loop logic excludes unrealistic/erronous measurements (skips-over/discards and uses next suitable for interpolation)
        # Note: No safety against fully infeasible dataframe beyond first sample (yet)
        while i < len(df):
            end = df.iloc[i] # Gets next potential destination point from raw measurements
            
            if not nx.has_path(G, tuple(start.graph_node), tuple(end.graph_node)):
                i += 1 # If unrealistic to drive this far, continue to the next GPS reading
                continue
            
            p = nx.shortest_path(G, tuple(start.graph_node), tuple(end.graph_node), 'weight')
            
            # After shortest path is found, calculate its length too
            del_l = 0
            for l in range(len(p)-1):
                del_l += min([G[p[l]][p[l+1]][e]['weight'] for e in G[p[l]][p[l+1]]])

            # Time it took the vehicle to get from A to B by route cost
            del_t = (end.timestamp - start.timestamp).seconds/3600

            if del_t < del_l: i += 1 # If unrealistic to drive this far, continue to the next GPS reading
            else: # Interpolate between the two readings
                t = start.timestamp.ceil('10S') # Synchronize first interpolated value to 10s intervals
                dt = (t - start.timestamp).seconds/3600 # Get time difference between real and interpolated reading
                node_id = 0 # Shortest path vector element (floored nearest)

                while t < end.timestamp: # Interpolate by 10s step intervals between 2 real readings
                    # Decrease dt by link weight between 2 consecutive path vector elements on the shortest path
                    while node_id < len(p)-1 and dt > min([G[p[node_id]][p[node_id+1]][e]['weight'] for e in G[p[node_id]][p[node_id+1]]]): 
                        dt -= min([G[p[node_id]][p[node_id+1]][e]['weight'] for e in G[p[node_id]][p[node_id+1]]])
                        node_id += 1

                    # Append interpolated value to data dict
                    [data[col].append(val) for col, val in zip(data, [t, *(p[node_id])])]

                    t += pd.Timedelta(10,'S') # Increment current time by 10s (interpolation step)
                    dt += step # Also increment remainder by 10s

                break # Go to next raw GPS reading

    df_new = pd.DataFrame(data=data)
    df_new.to_parquet(fn_out, engine='pyarrow')
    print('Successfully interpolated: {0}'.format(fn), flush=True, file=sys.stderr)
    
    return


if __name__ == '__main__':
    arg_parser = ArgumentParser(description='interpolates mobility data on per-bus basis')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    arg_parser.add_argument('--scheduler', help='scheduler host:port')
    options = arg_parser.parse_args()
    
    client = Client(str(options.scheduler))
    print('Client: {0}'.format(str(client)), flush=True, file=sys.stderr)

    in_path = '{0}/mobility/bus_data_snapped/'.format(options.dir)
    out_path = '{0}/mobility/bus_data_interpolated/'.format(options.dir)
    graph_url = '{0}/osm_roads/roads.json'.format(options.dir)

    futures = []
    for fn in os.listdir(in_path):
        if fn not in os.listdir(out_path):
            future = client.submit(process, in_path+fn, out_path+fn, fn, graph_url)
            futures.append(future)

    wait(futures)
