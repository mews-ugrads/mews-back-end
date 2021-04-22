#!/usr/bin/env python3

### Imports

from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import mysql.connector
import json
import os
from flask_cors import CORS, cross_origin


### Globals

app = Flask(__name__)
os.environ['FLASK_ENV'] = 'development'
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


### Constants

MEWS_CONFIG_FILEPATH = 'config/mews.json'
MEWSAPP_CONFIG_FILEPATH = 'config/mews-app.json'
DB_CONFIG_FILEPATH = 'config/inter-mews.json'


### Functions

def loadConfig(filepath):
    """
    @desc    Grabs JSON from file
    @return  JSON from filepath
    """
    with open(filepath) as f:
        return json.load(f)


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
    # Grab Mews-App Config
    mewsAppConfig = loadConfig(MEWSAPP_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        mewsAppCnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        return jsonify({'error': 'Could not connect to Mews-App DB'}), 400

    # Get Request Arguments
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=50)

    # Check Arguments
    try:
        assert(skip >= 0)
        assert(amount >= 0)
    except:
        return jsonify({'error': 'Invalid argument(s).'}), 400

    # Define Equation
    trendingEquation ='(10 * reposts + 10 * replies + likes)'

    # Query Mews-App DB
    mewsAppCursor = mewsAppCnx.cursor()
    query = ("SELECT id, image_url, post_url, reposts, replies, likes, when_posted, user_id FROM Posts "
    "WHERE when_posted BETWEEN %s AND %s "
    "ORDER BY %s DESC LIMIT %s, %s;")
    mewsAppCursor.execute(query, (lower_dt, upper_dt, trendingEquation, skip, amount))

    # Extract Information
    trendingPosts = []
    for result in mewsAppCursor.fetchall():
        (post_id, image_url, post_url, reposts, replies, likes, when_posted, user_id) = result
        post = {
                'id': post_id,
                'image_url': image_url,
                'post_url': post_url,
                'reposts': reposts,
                'replies': replies,
                'likes': likes,
                'when_posted': when_posted,
                'user_id': user_id
                }
        trendingPosts.append(post)

    mewsAppCnx.close()

    return jsonify(trendingPosts)

@app.route('/posts/<pid>', methods=['GET'])
def getPost(pid):
    """
    @route   GET /posts/<pid>
    @desc    Returns the specified post
    --
    @return  post
    """
    # Grab Mews-App Config
    mewsAppConfig = loadConfig(MEWSAPP_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        mewsAppCnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        return jsonify({'error': 'Could not connect to Mews-App DB'}), 400

    # Query Mews-App DB
    mewsAppCursor = mewsAppCnx.cursor()
    query = ("SELECT id, image_url, post_url, reposts, replies, likes, when_posted, user_id FROM Posts "
    "WHERE id = %s;")
    mewsAppCursor.execute(query, (pid,))

    # Extract Information
    result = mewsAppCursor.fetchone()
    if result is None:
        return jsonify({'error': 'Could not execute'}), 400

    (post_id, image_url, post_url, reposts, replies, likes, when_posted, user_id) = result
    post = {
            'id': post_id,
            'image_url': image_url,
            'post_url': post_url,
            'reposts': reposts,
            'replies': replies,
            'likes': likes,
            'when_posted': when_posted,
            'user_id': user_id
            }

    mewsAppCnx.close()

    return jsonify(post)

@app.route('/related/<pid>', methods=['GET'])
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

    # Grab Mews-App Config
    config = loadConfig(MEWSAPP_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        return jsonify({'error': 'Could not connect to Mews-App DB'}), 400

    # Constants to be Tuned
    REL_TXT_WEIGHT = 1
    SUB_IMG_WEIGHT = 1
    OCR_WEIGHT = 1

    # parse args
    try:
        pid = int(pid)
        assert(pid >= 0)
    except:
        return jsonify({'error': 'Invalid post id'}), 400

    try:
        skip = int(request.args.get('skip', 0))
        assert(skip >= 0)
    except:
        return jsonify({'error': 'Invalid parameter `skip`'}), 400

    try:
        amount = int(request.args.get('amount', 1))
        assert(amount >= 0)
    except:
        return jsonify({'error': 'Invalid parameter `amount`'}), 400

    # Query Mews-App DB
    cursor = cnx.cursor(dictionary=True)
    query = '''
        SELECT 
            post1_id,
            post2_id,
            rel_txt_wt,
            rel_txt_meta,
            ocr_meta,
            sub_img_wt,
            ocr_wt,
            ifnull(rel_txt_wt, 0) * %(rel_txt_wt)s + 
            ifnull(sub_img_wt, 0) * %(sub_img_wt)s + 
            ifnull(ocr_wt, 0) * %(ocr_wt)s 
            as total_wt
        FROM 
            PostRelatedness
        WHERE
            post1_id = %(post_id)s
            OR
            post2_id = %(post_id)s
        ORDER BY
            total_wt DESC
        LIMIT
            %(skip)s, %(amount)s 
        ;
    '''

    # Arguments for Query
    args = {
        'rel_txt_wt': REL_TXT_WEIGHT,
        'sub_img_wt': SUB_IMG_WEIGHT,
        'ocr_wt': OCR_WEIGHT,
        'post_id': pid,
        'skip': skip,
        'amount': amount
    }

    # Execute Query
    cursor.execute(query, args)

    # Process Results
    results = []
    for result in cursor.fetchall():
        try:
            # Find Non-Queried ID
            pids = set([int(result['post1_id']), int(result['post2_id'])])
            if (len(pids) == 1): continue
            pids -= set([pid])
            assert(len(pids) == 1)
            del result['post1_id']
            del result['post2_id']
            result['id'] = list(pids)[0] 

            results.append(result)
        except:
            pass

    # Clean Up
    cursor.close()
    cnx.close()

    # Return Results
    return jsonify(results)

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
    # Grab Mews-App Config
    config = loadConfig(DB_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        return jsonify({'error': 'Could not connect to DB'}), 400
    cursor = cnx.cursor()

    # Get Request Arguments
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=50)

    # Check Arguments
    try:
        assert(skip >= 0)
        assert(amount >= 0)
    except:
        return jsonify({'error': 'Invalid argument(s).'}), 400
    

    # Create Query
    # Grabs 'amount' number of ordered central nodes within time frame, then grabs ...
    # ... their post information, then grabs corresponding user info
    sql = '''
    SELECT 
        post.pid, post.image_url, post.post_url, 
        post.reposts, post.replies, post.likes, 
        post.when_posted, post.score, post.evaluated,
        IFNULL(username, 'UNKNOWN'), IFNULL(platform, 'UNKNOWN')
    FROM
        mews_app.Users,
        (SELECT
            id AS pid, image_url, post_url, 
            reposts, replies, likes, 
            when_posted, user_id,
            score, evaluated
        FROM
            mews_app.Posts,
            (SELECT
                post_id, score, evaluated
            FROM
                mews_app.PostCentrality
            WHERE
                evaluated BETWEEN %(lower_dt)s AND %(upper_dt)s
            ORDER BY
                score
            DESC
            LIMIT
                %(amount)s
            ) AS central
        WHERE
            central.post_id = id
        ) AS post
    WHERE
        post.user_id = id
    ;
    '''

    # Create Query Args
    args = {
        'lower_dt': lower_dt,
        'upper_dt': upper_dt,
        'amount': amount
    }
    
    # Perform Query
    cursor.execute(sql, args)

    # Extract Information
    centralPosts = []
    for result in cursor.fetchall():
        (post_id, image_url, post_url, reposts, replies, likes, when_posted, score, evaluated, username, platform) = result
        post = {
                'id': post_id,
                'image_url': image_url,
                'post_url': post_url,
                'reposts': reposts,
                'replies': replies,
                'likes': likes,
                'when_posted': when_posted,
                'score': score,
                'evaluated': evaluated,
                'username': username,
                'platform': platform
                }
        centralPosts.append(post)

    cnx.close()

    return jsonify(centralPosts)

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

    clusters = {}
    for row in cursor.fetchall():
        clusters[row['cluster_id']] = clusters.get(row['cluster_id'], []) + [{'post_id': row['post_id'], 'centrality': row['centrality']}]
    
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

    most_central_post = {cluster_id: max(cluster, key=lambda p: p['centrality']) for cluster_id, cluster in clusters.items()}

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
        ;
    '''

    for cluster_id, cluster in clusters.items():
        args = {
            'cluster_id': cluster_id
        }

        cursor.execute(sql, args)

        for edge in cursor.fetchall():
            out['links'].append({'source': edge['post1_id'], 'target': edge['post2_id']})

        representative_id = most_central_post[cluster_id]['post_id']
        out['links'].append({'source': representative_id, 'target': representative_id})

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

    cursor.close()
    cnx.close()

    if result is None:
        return jsonify({'error': 'No cluster information available'}), 400
    else:
        cid = result['id']
        return getClusters(cid)

### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
