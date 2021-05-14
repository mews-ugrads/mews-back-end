#!/usr/bin/env python3

### Imports

import mysql.connector
from datetime import datetime, timedelta
from networkx.algorithms import community
from networkx.algorithms import centrality
from tqdm import tqdm
import networkx as nx
import json
import sys

### Constants

MEWS_CONFIG_FILEPATH = 'config/inter-mews.json'

### Functions

def loadConfig(filepath):
    '''
    @desc   Loads the mysql config json files
    --
    @param  filepath  path to config file
    '''
    with open(filepath) as f:
        return json.load(f)

def connectSQL(config):
    '''
    @desc   Connects to mysql
    --
    @param  config  mysql.connector config object
    '''
    return mysql.connector.connect(**config)

def graph_from_db(cursor, begin_dt, end_dt):
    # Query to Gather Nodes & Edges
    sql = '''
        SELECT
            node1.id as node1_id,
            node2.id as node2_id,
            edge.total_wt as weight
        FROM
            mews_app.PostRelatedness as edge,
            mews_app.Posts as node1,
            mews_app.Posts as node2
        WHERE
            edge.post1_id = node1.id
            AND
            edge.post2_id = node2.id
            AND
            node1.when_posted BETWEEN %(begin_dt)s AND %(end_dt)s
            AND
            node2.when_posted BETWEEN %(begin_dt)s AND %(end_dt)s
            AND 
            edge.total_wt > 0
        ;
    '''

    # Arguments
    args = {
        'begin_dt': begin_dt,
        'end_dt': end_dt
    }

    # Run Query
    cursor.execute(sql, args)
    
    # Fetch Results
    edges = cursor.fetchall()
    nodes = set()
    for edge in edges:
        nodes.add(edge['node1_id'])
        nodes.add(edge['node2_id'])

    # Build Graph
    graph = nx.Graph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from((edge['node1_id'], edge['node2_id'], {'weight': edge['weight']}) for edge in edges)

    return graph

def daily_to_db(cursor, clustering_id, day):
    # Query
    sql = '''
        DELETE FROM
            mews_app.DailyClusterings
        WHERE
            day=%(day)s
        LIMIT 1
        ;
    '''

    # Arguments
    args = {
        'day': day
    }

    # Run Query
    cursor.execute(sql, args)

    sql = '''
        INSERT INTO
            mews_app.DailyClusterings
        (
            day,
            clustering_id
        )
        VALUES
        (
            %(day)s,
            %(clustering_id)s
        )
        ;
    '''

    # Arguments
    args = {
        'day': day,
        'clustering_id': clustering_id
    }

    # Run Query
    cursor.execute(sql, args)

def clustering_to_db(cursor):
    # Query
    sql = '''
        INSERT INTO
            mews_app.Clusterings
        (
            when_created
        )
        VALUES
        (
            %(when_created)s
        )
        ;
    '''

    # Arguments
    args = {
        'when_created': datetime.now()
    }

    # Run Query
    cursor.execute(sql, args)

    # Get ID of Inserted Clustering
    cursor.execute('SELECT LAST_INSERT_ID() as id;')
    id = cursor.fetchone()['id']

    return id

def cluster_to_db(cursor, clustering_id, cluster, centralities):
    # Query to Insert Cluster
    sql = '''
        INSERT INTO
            mews_app.Clusters
        (
            clustering_id
        )
        VALUES
        (
            %(clustering_id)s
        )
        ;
    '''

    # Arguments
    args = {
        'clustering_id': clustering_id
    }

    # Run Query
    cursor.execute(sql, args)

    # Get ID of Inserted Cluster
    cursor.execute('SELECT LAST_INSERT_ID() as id;')
    cluster_id = cursor.fetchone()['id']

    # Query to Connect Cluster With Post
    sql = '''
        INSERT INTO
            mews_app.PostsInClusters
        (
            post_id,
            cluster_id,
            centrality
        )
        VALUES
        (
            %(post_id)s,
            %(cluster_id)s,
            %(centrality)s
        )
        ;
    '''

    # Arguments
    args = [{'post_id': post_id, 'cluster_id': cluster_id, 'centrality': centralities[post_id]} for post_id in cluster]

    # Run Query
    cursor.executemany(sql, args)

    return cluster_id

def generate_clusters(graph):
    # Wrapper for Clustering Algorithm
    return community.asyn_lpa_communities(graph)

def cluster_centralities(graph, cluster):
    # Wrapper for Centrality Algorithm
    return centrality.betweenness_centrality(graph.subgraph(cluster), weight='weight')

def main():
    begin_dt = None
    end_dt = None 
    daily_dt = None   

    args = sys.argv[1:]
    date_format = '%Y-%m-%d'
    while len(args):
        arg = args.pop(0)
        if arg in ['--begin']:
            arg = args.pop(0)
            begin_dt = datetime.strptime(arg, date_format)
        elif arg in ['--end']:
            arg = args.pop(0)
            end_dt = datetime.strptime(arg, date_format)
        elif arg in ['--daily']:
            arg = args.pop(0)
            daily_dt = datetime.strptime(arg, date_format)
            if begin_dt is None:
                begin_dt = daily_dt - timedelta(6)
            if end_dt is None:
                end_dt = daily_dt + timedelta(1)

    if begin_dt is None or end_dt is None:
        print('please provide date range (--begin and --end, or --daily)', file=sys.stderr)
        exit(-1)

    # Grab Mews Config
    config = loadConfig(MEWS_CONFIG_FILEPATH)
    cnx = connectSQL(config)
    cursor = cnx.cursor(dictionary=True)

    # Load Graph
    graph = graph_from_db(cursor, begin_dt, end_dt)
    print(f'graph has {len(graph.nodes)} nodes, {len(graph.edges)} edges', file=sys.stderr)
  
    # Clusters to DB
    clustering_id = clustering_to_db(cursor)
    for cluster in tqdm(generate_clusters(graph)):
        if len(cluster) <= 4: 
            # Not Useful
            continue
        centralities = cluster_centralities(graph, cluster)

        cluster_to_db(cursor, clustering_id, cluster, centralities)
    if daily_dt is not None:
        daily_to_db(cursor, clustering_id, daily_dt)
    cnx.commit()

    # Clean Up
    cursor.close()
    cnx.close()

# Main Execution

if __name__ == '__main__':
    main()