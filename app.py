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
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=50)

    # Call Internal Function
    trendPosts, code = Posts.getTrendingPosts(upper_dt, lower_dt, skip, amount)

    return jsonify(trendPosts, code)


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
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    central_amount = request.args.get('central_amount', type=int, default=10)
    rel_amount = request.args.get('rel_amount', type=int, default=10)

    # Call Function
    graph, code = Graph.getCentralGraph(upper_dt, lower_dt, skip, central_amount, rel_amount)

    return jsonify(graph), code


### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
