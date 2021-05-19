from dask.distributed import Client, wait
import dask.dataframe as dd
import os
from shapely.geometry import LineString, Polygon, Point, box
from shapely import wkb
import rtree
import xarray as xr
import pandas as pd
import pyarrow as pa

index_url = './../data/roads'
df_url = './../data/osm_roads/roads.parquet'
dem_url = './../data/elevation/mergedReprojDEM.tif'

client = Client('192.168.0.134:8786')


def get_neighbors(index, row):
    # Query R-Tree by the bounding box of road 'x' for neighbors except itself
    return [int(i) for i in index.intersection(wkb.loads(row.bbox).bounds) if int(i) != row.name]


def find_intersections(is_neighbors, neighbors, row):
    intersections = []  # Container for street intersections
    nodes = []  # Arrays of tuples for NetworkX MultiDiGraph
    a = wkb.loads(row.geometry)
    road = a.coords[:]

    if is_neighbors:
        for entry in neighbors.itertuples():
            b = wkb.loads(entry.geometry)
            # Check if road with 'fid' osm_id actually intersects road 'x'
            if not (entry.bridge or entry.tunnel) and a.intersects(b):
                pts = a.intersection(b)
                if pts.type == 'MultiPoint':
                    (nodes.append((pt.coords[:][0], {'junction': [row.name, entry.Index]})) for pt in pts)
                    (intersections.append(pt) for pt in pts if pt.coords[:][0] != road[0] and pt.coords[:][0] != road[-1] and (pt.coords[:][0] not in intersections))
                elif pts.type == 'Point':
                    nodes.append((pts.coords[:][0], {'junction': [row.name, entry.Index]}))
                    if pts.coords[:][0] != road[0] and pts.coords[:][0] != road[-1] and (pts.coords[:][0] not in intersections):
                        intersections.append(pts)

    [nodes.append((pt, {'junction': [row.name]})) for pt in [road[0], road[-1]] if not nodes or pt not in tuple(zip(*nodes))[0]]

    return nodes, intersections


def get_elevation(nodes, a, dem):
    # List coordinates of graph nodes (permits duplication)
    b = [pt[0] for pt in nodes]

    data = dem[tuple([[0]]+list(map(list, zip(*((round((pt[0]-dem.transform[2])/dem.transform[0]), round((pt[1]-dem.transform[5])/dem.transform[4])) for pt in a+b)))))].data[0].compute(scheduler='single-threaded')  # Retrieve elevations for road points and graph nodes

    # The first 'a' elements correspond to road points
    road_elevations = [data[i][i] for i in range(len(a))]

    for i in range(len(b)):
        # The second 'b' elements correspond to graph nodes
        nodes[i][1]['altitude'] = data[len(a)+i][len(a)+i]

    return nodes, road_elevations


def compute_edges(intersections, nodes, road_elevations, row):
    road = wkb.loads(row.geometry).coords[:]
    edges = []
    segment_len = 0
    segment_flatness = 0
    previous_node_elevation = road_elevations[0]
    # Coordinate keeping track of previous intersection/edge end
    previous_node = road[0]

    for idx in range(len(road)-1):
        # LineString of straight line segment between two consecutive points
        segment = LineString(road[idx:idx+2])
        # Coordinate updating on every segment or when intersection encountered
        segment_start = road[idx]
        queue = []  # Point objects that intersect this particular road straight line segment

        for pt in list(intersections):
            if segment.intersects(pt):
                # Put all junctions intersecting this segment into a queue
                queue.append(pt)
                # Remove the junction from left-over list of street intersections
                intersections.remove(pt)

        if not queue:
            # If no junctions in this road segment, increase length by distance between LineString consecutive points
            segment_len += segment.length
            next_node_elevation = road_elevations[idx+1]
            segment_flatness += (previous_node_elevation - next_node_elevation) ** 2
            previous_node_elevation = next_node_elevation
        else:
            for pt in list(queue):
                line_lengths = [LineString([segment_start, p.coords[:][0]]).length for p in queue]
                shortest_line = min(line_lengths)
                next_node_idx = [k for k, l in enumerate(line_lengths) if l == shortest_line][0]
                next_node = queue[next_node_idx].coords[:][0]
                segment_len += LineString([segment_start, next_node]).length

                if segment_len:  # Multiple roads crossing at the same junction. Can skip. osm_id's on intersectinos are maintained by nodes array
                    next_node_elevation = [node[1]['altitude'] for node in nodes if node[0] == next_node][0]
                    segment_flatness += (previous_node_elevation - next_node_elevation) ** 2
                    previous_node_elevation = next_node_elevation

                    edges.append((
                        previous_node,
                        next_node,
                        {
                            'length': segment_len,
                            'weight': segment_len/row.maxspeed/1000,
                            'flatness': segment_flatness/segment_len
                        }))

                    if not row.oneway:  # If both way street, add identical reverse relation between MultiDiGraph nodes
                        edges.append((
                            next_node,
                            previous_node,
                            {
                                'length': segment_len,
                                'weight': segment_len/row.maxspeed/1000,
                                'flatness': segment_flatness/segment_len
                            }))

                segment_len = 0
                segment_flatness = 0
                previous_node = next_node
                segment_start = next_node
                # Remove the junction from the queue
                queue.remove(queue[next_node_idx])

            # Get distance to the endpoint of the segment
            segment_len += LineString([segment_start, road[idx+1]]).length
            next_node_elevation = road_elevations[idx+1]
            segment_flatness += (previous_node_elevation - next_node_elevation) ** 2
            previous_node_elevation = next_node_elevation

    edges.append((
        previous_node,
        road[-1],
        {
            'length': segment_len,
            'weight': segment_len/row.maxspeed/1000,
            'flatness': segment_flatness/segment_len
        }))

    if not row.oneway:  # If both way street, add identical reverse relation between MultiDiGraph nodes
        edges.append((
            road[-1],
            previous_node,
            {
                'length': segment_len,
                'weight': segment_len/row.maxspeed/1000,
                'flatness': segment_flatness/segment_len
            }))

    return edges


def foo(row, df, dem, index):
    neighbors = None
    is_neighbors = False

    # Assumption that bridges and tunnels do not have intersections
    if not (row.bridge or row.tunnel):
        # Retreive from R-tree osm_id's of roads whose bounding box overlaps this road's
        fids = get_neighbors(index, row)
        # Retreive those roads from the dataset by indexing
        neighbors = df.loc[fids].compute(scheduler='single-threaded')
        is_neighbors = True

    # Build up list of Graph nodes and list of intersections
    (nodes, intersections) = find_intersections(is_neighbors, neighbors, row)

    # Retrieves elevation data for all road points and graph nodes
    (nodes, road_elevations) = get_elevation(nodes, wkb.loads(row.geometry).coords[:], dem)

    # Calculate graph edges between junction nodes
    edges = compute_edges(intersections, nodes, road_elevations, row)

    return nodes, edges


def process(fn_in, fn_out, df_url, dem_url, index_url, schema):
    df = dd.read_parquet(df_url, engine='pyarrow')
    dem = xr.open_rasterio(dem_url, chunks={'band': 1, 'x': 3500, 'y': 4000})
    d = pd.read_parquet(fn_in)
    index = rtree.index.Rtree(index_url)

    d[['nodes', 'edges']] = d.apply(
        foo,
        args=(df, dem, index),
        axis=1,
        result_type='expand')

    print('Writing processed data to '+fn_out)
    d.to_parquet(fn_out, engine='pyarrow', schema=schema)

    del df, dem, d, index

    return

schema = pa.schema([
    ('osm_id', pa.int64()),
    ('code', pa.int64()),
    ('fclass', pa.string()),
    ('road_name', pa.string()),
    ('ref', pa.string()),
    ('oneway', pa.bool_()),
    ('maxspeed', pa.int64()),
    ('layer', pa.int64()),
    ('bridge', pa.bool_()),
    ('tunnel', pa.bool_()),
    ('geometry', pa.binary()),
    ('bbox', pa.binary()),
    ('nodes', pa.list_(
        pa.struct([
            ('0', pa.list_(pa.float64(), 2)),
            ('1', pa.struct([
                ('junction', pa.list_(pa.int64())),
                ('altitude', pa.int64()),
            ]))
        ])
    )),
    ('edges', pa.list_(
        pa.struct([
            ('0', pa.list_(pa.float64(), 2)),
            ('1', pa.list_(pa.float64(), 2)),
            ('2', pa.struct([
                ('length', pa.float64()),
                ('weight', pa.float64()),
                ('flatness', pa.float64()),
            ]))
        ])
    ))
])

in_path = './../data/osm_roads/roads_partition.parquet/'
out_path = './../data/osm_roads/roads_intersected.parquet/'
futures = []
for fn in os.listdir(in_path):
    if fn not in os.listdir(out_path):
        future = client.submit(process, in_path + fn, out_path + fn, df_url, dem_url, index_url, schema)
        futures.append(future)

wait(futures)
