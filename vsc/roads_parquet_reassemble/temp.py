#!/usr/bin/env python

from argparse import ArgumentParser
from dask.distributed import Client
import dask.dataframe as dd
import dask.delayed as delayed
import pyarrow as pa
import pandas as pd
import numpy as np
import sys
import os
import re

if __name__ == '__main__':
    arg_parser = ArgumentParser(description='resets index of intersected Parquet files and composes Dask Dataframe compatible Parquet')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    arg_parser.add_argument('--scheduler', help='scheduler host:port')
    options = arg_parser.parse_args()
    
    client = Client(str(options.scheduler))
    print('Client: {0}'.format(str(client)), flush=True, file=sys.stderr)
    
    in_path = '{0}/osm_roads/roads_intersected.parquet'.format(options.dir)
    out_path = '{0}/osm_roads/roads_new2.parquet'.format(options.dir)

    schema = pa.schema([
        ('oneway', pa.bool_()),
        ('bridge', pa.bool_()),
        ('tunnel', pa.bool_()),
        ('nodes', pa.list_(
            pa.list_(pa.float64(), 2)
        )),
        ('osm_id', pa.int64()),
    ])

    l = [int(re.split('(part)|\.',fn)[2]) for fn in os.listdir(in_path)]
    l.sort()
    fns = [f'part{i}.parquet' for i in l]

    dfs = [delayed(pd.read_parquet)(f'{in_path}/{fn}', engine='pyarrow') for fn in fns]

    ddf = dd.from_delayed(dfs)

    ddf2 = ddf[['oneway','bridge','tunnel']]
    ddf2['nodes'] = ddf.apply(
        lambda row: [tuple(node['0']) for node in row.nodes],
        axis=1,
        meta=('nodes','f8')
    )

    dd.to_parquet(ddf2, out_path, engine='pyarrow', schema=schema)
