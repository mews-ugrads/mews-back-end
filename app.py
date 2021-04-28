#!/usr/bin/env python3

### Imports

from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import mysql.connector
import json
import os
from flask_cors import CORS, cross_origin
from MewsUtils import Posts, Graph


### Globals

app = Flask(__name__)
os.environ['FLASK_ENV'] = 'development'
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


### API Routes

@app.route('/posts/trending', methods=['GET'])
def getTrending():
    """
    @route   GET /posts/trending
    @desc    Returns the trending posts within timeline
    --
    @param   skip   - number of posts to skip (int)
    @param   amount - number of posts to return (int)
    @param   lower  - lower bound for when_posted (datetime syntax)
    @param   upper  - upper bound for when_posted (datetime syntax)
    --
    @return  list of trending posts
    """

    # Get Request Arguments
    upper = request.args.get('upper', type=str, default = str(datetime.now()))
    lower = request.args.get('lower', type=str, default = str(datetime.now() - timedelta(days=30)))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=10)

    # Call Internal Function
    trendPosts, code = Posts.getTrendingPosts(upper, lower, skip, amount)

    return jsonify(trendPosts), code


@app.route('/posts/<pid>', methods=['GET'])
def getPost(pid):
    """
    @route   GET /posts/<pid>
    @desc    Returns the specified post
    --
    @return  post
    """

    # Call Internal Function
    post, code = Posts.getPost(pid)
 
    return jsonify(post), code


@app.route('/posts/<pid>/related', methods=['GET'])
def getRelatedPosts(pid):
    """
    @route   GET /related/<pid>
    @desc    Returns the specified post
    --
    @param   skip   - number of posts to skip (int)
    @param   amount - number of posts to return (int)
    --
    @return  list of related posts
    """

    # Grab Request Arguments
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=3)

    # Call Internal Function
    posts, code = Posts.getRelatedPosts(pid, skip, amount)

    return jsonify(posts), code


@app.route('/posts/central', methods=['GET'])
def getCentralPosts():
    """
    @route   GET /posts/central
    @desc    Returns the central posts
    --
    @param   skip   - number of posts to skip (int)
    @param   amount - number of posts to return (int)
    @param   lower  - lower bound for when_posted (datetime syntax)
    @param   upper  - upper bound for when_posted (datetime syntax)
    --
    @return  list of central posts
    """

    # Get Request Arguments
    upper = request.args.get('upper', type=str, default = str(datetime.now()))
    lower = request.args.get('lower', type=str, default = str(datetime.now() - timedelta(days=30)))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=3)

    # Call Function
    centralPosts, code = Posts.getCentralPosts(upper, lower, skip, amount)

    return jsonify(centralPosts), code


@app.route('/graph/central', methods=['GET'])
def getCentralGraph():
    """
    @route   GET /graph/central
    @desc    Returns the central posts and related posts and links
    --
    @param   skip           - number of posts to skip (int)
    @param   central_amount - number of central posts to return (int)
    @param   rel_amount     - number of related posts per central post to return (int)
    @param   lower          - lower bound for when_posted (datetime syntax)
    @param   upper          - upper bound for when_posted (datetime syntax)
    --
    @return  list of central posts and related posts with links
    """

    # Get Request Arguments
    upper = request.args.get('upper', type=str, default = str(datetime.now()))
    lower = request.args.get('lower', type=str, default = str(datetime.now() - timedelta(days=30)))
    skip = request.args.get('skip', type=int, default=0)
    central_amount = request.args.get('central_amount', type=int, default=10)
    rel_amount = request.args.get('rel_amount', type=int, default=10)

    # Call Function
    graph, code = Graph.getCentralGraph(upper, lower, skip, central_amount, rel_amount)

    return jsonify(graph), code


@app.route('/clusters/<cid>', methods=['GET'])
def getClusters(cid):
    # Grab Mews-App Config
    config = loadConfig(DB_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error:
        return jsonify({'error': 'Could not connect to DB'}), 400
    cursor = cnx.cursor(dictionary=True)

    # Request Params
    amount = request.args.get('amount', type=int, default=-1)

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
            image_url as svg,
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
            result.update({'id': post['post_id'], 'centrality': post['centrality']})
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

@app.route('/clusters/recent', methods=['GET'])
def getRecentClusters():
    # Grab Mews-App Config
    config = loadConfig(DB_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error:
        return jsonify({'error': 'Could not connect to DB'}), 400
    cursor = cnx.cursor(dictionary=True)

    # Query ID of Most Recent Clustering
    sql = '''
        SELECT
            id
        FROM
            mews_app.Clusterings
        ORDER BY
            when_created DESC
        LIMIT 1
        ;
    '''

    cursor.execute(sql)

    result = cursor.fetchone()

    # Clean Up
    cursor.close()
    cnx.close()

    # Determine Results
    if result is None:
        return jsonify({'error': 'No cluster information available'}), 400
    else:
        cid = result['id']
        return getClusters(cid)

### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
