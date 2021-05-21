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
    
    # TODO: interpolation logic
