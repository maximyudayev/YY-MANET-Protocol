#!/usr/bin/env python

from argparse import ArgumentParser
from distributed import Client
import dask.dataframe as dd
import xarray as xr
from shapely.geometry import LineString, Polygon, Point, box
from shapely import wkb
import rtree
import networkx as nx
import sys


def get_neighbors(index, row):
    # Query R-Tree by the bounding box of road 'x' for neighbors except itself
    return [int(i) for i in index.intersection(wkb.loads(row.bbox).bounds) if int(i) != row.name]


def find_intersections(neighbors, row):
    intersections = [] # Container for street intersections    
    nodes = [] # Arrays of tuples for NetworkX MultiDiGraph
    a = wkb.loads(row.geometry)
    road = a.coords[:]
    
    for entry in neighbors.itertuples():
        b = wkb.loads(entry.geometry)
        if not (entry.bridge or entry.tunnel) and a.intersects(b): # Check if road with 'fid' osm_id actually intersects road 'x'
            pts = a.intersection(b)
            if pts.type == 'MultiPoint':
                (nodes.append((pt.coords[:][0], {'junction':[row.name, entry.Index]})) for pt in pts)
                (intersections.append(pt) for pt in pts if pt.coords[:][0] != road[0] and pt.coords[:][0] != road[-1] and (pt.coords[:][0] not in intersections))
            elif pts.type == 'Point':
                nodes.append((pts.coords[:][0], {'junction':[row.name, entry.Index]}))
                if pts.coords[:][0] != road[0] and pts.coords[:][0] != road[-1] and (pts.coords[:][0] not in intersections):
                    intersections.append(pts)
    
    [nodes.append((pt, {'junction':[row.name]})) for pt in [road[0], road[-1]] if not nodes or pt not in tuple(zip(*nodes))[0]]

    return nodes, intersections


def get_elevation(nodes, a, dem):
    b = [pt[0] for pt in nodes] # List coordinates of graph nodes (permits duplication)
    
    data = dem[tuple([[0]]+list(map(list,zip(*((round((pt[0]-dem.transform[2])/dem.transform[0]), round((pt[1]-dem.transform[5])/dem.transform[4])) for pt in a+b)))))].data[0] # Retrieve elevations for road points and graph nodes
    
    road_elevations = [data[i][i] for i in range(len(a))] # The first 'a' elements correspond to road points
    
    for i in range(len(b)):
        nodes[i][1]['altitude'] = data[len(a)+i][len(a)+i] # The second 'b' elements correspond to graph nodes
    
    return nodes, road_elevations


def compute_edges(intersections, nodes, road_elevations, row):
    road = wkb.loads(row.geometry).coords[:]
    edges = []
    segment_len = 0
    segment_flatness = 0
    previous_node_elevation = road_elevations[0]
    previous_node = road[0] # Coordinate keeping track of previous intersection/edge end

    for idx in range(len(road)-1):
        segment = LineString(road[idx:idx+2]) # LineString of straight line segment between two consecutive points
        segment_start = road[idx] # Coordinate updating on every segment or when intersection encountered
        queue = [] # Point objects that intersect this particular road straight line segment

        for pt in list(intersections):
            if segment.intersects(pt):
                queue.append(pt) # Put all junctions intersecting this segment into a queue
                intersections.remove(pt) # Remove the junction from left-over list of street intersections

        if not queue:
            segment_len += segment.length # If no junctions in this road segment, increase length by distance between LineString consecutive points
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

                if segment_len: # Multiple roads crossing at the same junction. Can skip. osm_id's on intersectinos are maintained by nodes array
                    next_node_elevation = [node[1]['altitude'] for node in nodes if node[0] == next_node][0]
                    segment_flatness += (previous_node_elevation - next_node_elevation) ** 2 
                    previous_node_elevation = next_node_elevation

                    edges.append((
                        previous_node,
                        next_node,
                        {
                            'length':segment_len,
                            'weight':segment_len/row.maxspeed/1000,
                            'flatness':segment_flatness/segment_len
                        }))

                    if not row.oneway: # If both way street, add identical reverse relation between MultiDiGraph nodes
                        edges.append((
                            next_node,
                            previous_node,                          
                            {
                                'length':segment_len,
                                'weight':segment_len/row.maxspeed/1000,
                                'flatness':segment_flatness/segment_len
                            }))

                segment_len = 0
                segment_flatness = 0
                previous_node = next_node
                segment_start = next_node
                queue.remove(queue[next_node_idx]) # Remove the junction from the queue

            segment_len += LineString([segment_start, road[idx+1]]).length # Get distance to the endpoint of the segment
            next_node_elevation = road_elevations[idx+1]
            segment_flatness += (previous_node_elevation - next_node_elevation) ** 2 
            previous_node_elevation = next_node_elevation

    edges.append((
        previous_node,
        road[-1],
        {
            'length':segment_len,
            'weight':segment_len/row.maxspeed/1000,
            'flatness':segment_flatness/segment_len
        }))

    if not row.oneway: # If both way street, add identical reverse relation between MultiDiGraph nodes
        edges.append((
            road[-1],
            previous_node,            
            {
                'length':segment_len,
                'weight':segment_len/row.maxspeed/1000,
                'flatness':segment_flatness/segment_len
            }))
        
    return edges


def foo(row, df, dem, index_url):
    fids = []
    
    # Assumption that bridges and tunnels do not have intersections
    if not (row.bridge or row.tunnel): 
        index = rtree.index.Rtree(index_url)
        
        # Retreive from R-tree osm_id's of roads whose bounding box overlaps this road's
        fids = get_neighbors(index, row)
    else:
        # If bridge or tunnel, only intersects others at start/end of way
        fids = [] 

    # Retreive those roads from the dataset by indexing
    neighbors = df.loc[fids]

    # Build up list of Graph nodes and list of intersections
    (nodes, intersections) = find_intersections(neighbors, row)

    # Retrieves elevation data for all road points and graph nodes
    (nodes, road_elevations) = get_elevation(nodes, wkb.loads(row.geometry).coords[:], dem)

    # Calculate graph edges between junction nodes
    edges = compute_edges(intersections, nodes, road_elevations, row)

    return nodes, edges


if __name__ == '__main__':
    arg_parser = ArgumentParser(description='construct road network weighted Graph '
                                            'from the distributed Dask Dataframe and '
					    'XArray Digital Elevation Model')
    arg_parser.add_argument('dir', help='scratch directory with data files')
    arg_parser.add_argument('--scheduler', help='scheduler host:port')
    options = arg_parser.parse_args()
    
    client = Client(str(options.scheduler))
    print('Client: {0}'.format(str(client)), flush=True, file=sys.stderr)
    
    index_url = '{0}/roads'.format(options.dir)
    df_url = '{0}/osm_roads/roads_new.parquet'.format(options.dir)
    dem_url = '{0}/elevation/mergedReprojDEM.tif'.format(options.dir)

    df = dd.read_parquet(df_url, engine='pyarrow')
    dem = xr.open_rasterio(dem_url, chunks={'band':1, 'x': 3500, 'y': 4000})

    # df[['nodes','edges']] = df.apply( # Process full set
    df2 = df.loc[4217292:4331539].apply( # Process first 100 roads
        foo, 
        args=(df, dem, index_url), 
        axis=1, 
        result_type='expand', 
        meta={0:'object',1:'object'})

    # dd.to_parquet(df, '{0}/osm_roads/nodes_edges.parquet'.format(options.dir), engine='pyarrow') # Process full set
    dd.to_parquet(df2, '{0}/osm_roads/debug_nodes_edges.parquet'.format(options.dir), engine='pyarrow') # Process first 100 roads
