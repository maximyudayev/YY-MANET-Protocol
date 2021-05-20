#!/usr/bin/env python

from argparse import ArgumentParser
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import sys


if __name__ == '__main__':
    arg_parser = ArgumentParser(description='splits Parquet bus data into subsets on per-bus basis')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    arg_parser.add_argument('--scheduler', help='scheduler host:port')
    options = arg_parser.parse_args()
    
    client = Client(str(options.scheduler))
    print('Client: {0}'.format(str(client)), flush=True, file=sys.stderr)
    
    in_path = '{0}/mobility/bus_data_clean.parquet'.format(options.dir)
    out_path = '{0}/mobility/bus_data_clean_split'.format(options.dir)

    orders = '{0}/mobility/orders.csv'.format(options.dir)
    df = dd.read_parquet(in_path, engine='pyarrow')

    for i in range(48, len(orders)):
        df_out = df[df.order == i].compute()
        df_out.to_parquet(f'{out_path}/{i}.parquet', engine='pyarrow')
