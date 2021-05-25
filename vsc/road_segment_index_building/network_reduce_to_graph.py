#!/usr/bin/env python

import os
import sys
import pandas as pd
from shapely import wkb
import rtree
from argparse import ArgumentParser

if __name__ == '__main__':
    arg_parser = ArgumentParser(description='creates R-tree index for all road points')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    options = arg_parser.parse_args()

    # Create R-tree index of all roads data points to snap GPS data to the closest road point
    in_path = '{0}/osm_roads/roads_intersected.parquet/'.format(options.dir)
    index_road_segments = rtree.index.Rtree('{0}/road_segments'.format(options.dir))
    for i, fn in enumerate(os.listdir(in_path)):
        df = pd.read_parquet(in_path+fn, engine='pyarrow')
        print('{0}: Loading {1} into R-tree'.format(i,fn), flush=True, file=sys.stderr)
        for row in df.itertuples():
            for pt in wkb.loads(row.geometry).coords[:]:
                # Associates indexed objects with osm_id of the corresponding road
                index_road_segments.insert(row.Index, pt)

    index_road_segments.close() # Don't forget to close the index to save changes
