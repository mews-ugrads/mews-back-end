#!/usr/bin/env python3


### Imports

import json
import os
import mysql.connector
from flask import jsonify
from . import Connection, Images

### Functions

def getDailyClusters(day, amount):
     # Connect to DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor(dictionary=True)

    # Query ID of Relevant Clustering
    sql = '''
        SELECT
            clustering_id
        FROM
            mews_app.DailyClusterings
        WHERE
            day=%(day)s
        ;
    '''

    args = {'day': day}

    cursor.execute(sql, args)

    result = cursor.fetchone()

    # Clean Up
    cursor.close()
    cnx.close()

    # Determine Results
    if result is None:
        return {'error': 'No cluster information available'}, 400
    else:
        return getClusters(result['clustering_id'], amount), 200

def getClusters(cid, amount):
    # Connect to Mews-App DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error:
        return jsonify({'error': 'Could not connect to DB'}), 400
    cursor = cnx.cursor(dictionary=True)

    # Query Nodes
    sql = '''
        SELECT
            PostsInClusters.cluster_id as cluster_id,
            PostsInClusters.post_id as post_id,
            PostsInClusters.centrality as centrality
        FROM
            mews_app.Clusters,
            mews_app.PostsInClusters
        WHERE
            Clusters.clustering_id = %(clustering_id)s
            AND
            PostsInClusters.cluster_id = Clusters.id
        ;
    '''

    args = {
        'clustering_id': cid
    }

    cursor.execute(sql, args)

    # Format Clusters
    clusters = {}
    for row in cursor.fetchall():
        clusters[row['cluster_id']] = clusters.get(row['cluster_id'], []) + [{'post_id': row['post_id'], 'centrality': row['centrality']}]
    
    # Limit (Sorted) Clusters
    clusters = list(sorted(clusters.items(), key=lambda t: len(t[1]), reverse=True))
    if amount is not None and len(clusters) > amount:
        clusters = clusters[:amount]
    clusters = dict(clusters)

    # Query Post Information
    sql = '''
        SELECT 
            post_url
        FROM
            mews_app.Posts
        WHERE 
            id = %(post_id)s
        ;
    '''

    # Get Information for Each Post, Add to Output
    out = {'nodes':[], 'links':[]}
    for cluster in clusters.values():
        for post in cluster:
            args = {
                'post_id': post['post_id']
            }

            cursor.execute(sql, args)

            result = cursor.fetchone()
            result.update({'id': post['post_id'], 'centrality': post['centrality'], 'svg': Images.getImageURL(post["post_id"])})
            out['nodes'].append(result)

    # Determine Central Post of Each Cluster
    most_central_post = {cluster_id: max(cluster, key=lambda p: p['centrality']) for cluster_id, cluster in clusters.items()}

    # Query Edges
    sql = '''
        SELECT
            PostRelatedness.post1_id as post1_id,
            PostRelatedness.post2_id as post2_id,
            PostRelatedness.total_wt as weight
        FROM
            mews_app.Clusters,
            mews_app.PostsInClusters as PostsInClusters1,
            mews_app.PostsInClusters as PostsInClusters2,
            mews_app.PostRelatedness
        WHERE
            Clusters.id = %(cluster_id)s
            AND
            PostsInClusters1.cluster_id = Clusters.id
            AND
            PostsInClusters2.cluster_id = Clusters.id
            AND
            PostRelatedness.post1_id = PostsInClusters1.post_id
            AND
            PostRelatedness.post2_id = PostsInClusters2.post_id
            AND
            PostRelatedness.total_wt > 0
        ;
    '''

    # Add Edges To Output
    for cluster_id, cluster in clusters.items():
        args = {
            'cluster_id': cluster_id
        }

        cursor.execute(sql, args)

        for edge in cursor.fetchall():
            out['links'].append({'source': edge['post1_id'], 'target': edge['post2_id'], 'weight': edge['weight']})

        representative_id = most_central_post[cluster_id]['post_id']
        out['links'].append({'source': representative_id, 'target': representative_id})

    # Clean Up
    cursor.close()
    cnx.close()

    return jsonify(out)