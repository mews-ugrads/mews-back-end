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
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=3)

    # Call Function
    centralPosts, code = Posts.getCentralPosts(upper_dt, lower_dt, skip, amount)

    return jsonify(centralPosts), code


@app.route('/graph/central', methods=['GET'])
def getCentralGraph():
    """
    @route   GET /graph/central
    @desc    Returns the central posts
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
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    central_amount = request.args.get('amount', type=int, default=10)
    rel_amount = request.args.get('amount', type=int, default=10)

    # Call Function
    graph, code = Graph.getCentralGraph(upper_dt, lower_dt, skip, central_amount, rel_amount)

    return jsonify(graph), code

    


### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
