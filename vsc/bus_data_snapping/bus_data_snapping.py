#!/usr/bin/env python

from argparse import ArgumentParser
from dask.distributed import Client, wait
import dask.dataframe as dd
import pandas as pd
import numpy as np
import pyarrow as pa
import rtree
import os
import sys
from pyproj import Transformer


def foo(row, ddf, transformer, index_road_segments):
    pt = transformer.transform(row.longitude, row.latitude)
    hit = list(index_road_segments.nearest(pt, objects=True))[0]
    pt = tuple(hit.bbox[:2]) # Snaps GPS to nearest road segment
    nodes = ddf.loc[hit.id].compute(scheduler='single-threaded') # Indicates which road the snapped-to segment belongs to
    
    # Selects the closest Graph node
    nodes = np.asarray([tuple(node) for node in nodes.iloc[0].nodes])
    dists = np.sum((nodes-pt)**2, axis=1)
    graph_node = nodes[np.argmin(dists)]
    
    return pt, graph_node


def process(fn_in, fn_out, ddf_url, index_url, schema):
    ddf = dd.read_parquet(ddf_url, engine='pyarrow', columns=['nodes', 'oneway', 'bridge', 'tunnel'])
    df = pd.read_parquet(fn_in, engine='pyarrow')
    index = rtree.index.Rtree(index_url)
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    
    print(f'Starting processing {fn_in}', flush=True, file=sys.stderr)

    df[['snapped_coordinate','graph_node']] = df.apply(
        foo,
        args=(ddf, transformer, index),
        axis=1,
        result_type='expand')

    print(f'Writing processed data to {fn_out}', flush=True, file=sys.stderr)

    df.to_parquet(fn_out, engine='pyarrow', schema=schema)

    del ddf, df, index, transformer

    return


if __name__ == '__main__':
    arg_parser = ArgumentParser(description='snaps bus GPS points to road and attaches route to intersection')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    arg_parser.add_argument('--scheduler', help='scheduler host:port')
    options = arg_parser.parse_args()
    
    client = Client(str(options.scheduler))
    print('Client: {0}'.format(str(client)), flush=True, file=sys.stderr)
    
    index_url = '{0}/road_segments'.format(options.dir)
    ddf_url = '{0}/osm_roads/roads_new.parquet'.format(options.dir)
    in_path = '{0}/mobility/bus_data_clean_split/'.format(options.dir)
    out_path = '{0}/mobility/bus_data_snapped/'.format(options.dir)


    schema = pa.schema([
        ('timestamp', pa.timestamp('s')),
        ('order', pa.int64()),
        ('line', pa.int64()),
        ('latitude', pa.float64()),
        ('longitude', pa.float64()),
        ('speed', pa.float64()),
        ('snapped_coordinate', pa.list_(pa.float64(), 2)),
        ('graph_node', pa.list_(pa.float64(), 2)),
    ])


    futures = []    
    for fn in os.listdir(in_path):
        if fn not in os.listdir(out_path):
            print(f'Submitted: {fn}', flush=True, file=sys.stderr)
            future = client.submit(process, in_path+fn, out_path+fn, ddf_url, index_url, schema)
            futures.append(future)

    wait(futures)

