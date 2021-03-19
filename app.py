#!/usr/bin/env python3

### Imports

from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import mysql.connector
import json
import os


### Globals

app = Flask(__name__)
os.environ['FLASK_ENV'] = 'development'

### Constants

MEWS_CONFIG_FILEPATH = 'config/mews.json'
MEWSAPP_CONFIG_FILEPATH = 'config/mews-app.json'

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

    # Get Timeline
    upper_dt = request.args.get('upper', default = datetime.now())
    lower_dt = request.args.get('lower', default = datetime.now() - timedelta(days=30))

    # Get Amount
    skip = request.args.get('skip', default=0)
    amount = request.args.get('amount', default=50)

    # Define Equation
    trendingEquation = '(10 * reposts + 10 * replies + likes)'

    # Query Mews-App DB
    mewsAppCursor = mewsAppCnx.cursor()
    query = ("SELECT id, image_url, post_url, reposts, replies, likes, when_posted FROM Posts "
    "WHERE when_posted BETWEEN %s AND %s "
    "ORDER BY %s DESC LIMIT %s, %s;")
    mewsAppCursor.execute(query, (lower_dt, upper_dt, trendingEquation, skip, amount))

    # Extract Information
    trendingPosts = []
    for result in mewsAppCursor.fetchall():
        (post_id, image_url, post_url, reposts, replies, likes, when_posted) = result
        post = {
                'id': post_id,
                'image_url': image_url,
                'post_url': post_url,
                'reposts': reposts,
                'replies': replies,
                'likes': likes,
                'when_posted': when_posted
                }
        trendingPosts.append(post)

    mewsAppCnx.close()

    return jsonify(trendingPosts)

@app.route('/posts/<pid>', methods=['GET'])
def getPost():
    """
    @route   GET /posts/<pid>
    @desc    Returns the specified post
    --
    @return  post
    """


### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
