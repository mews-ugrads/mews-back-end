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

    # Query Mews-App DB
    cursor = cnx.cursor(dictionary=True)
    query = '''
        SELECT 
            post1_id,
            post2_id,
            rel_txt_wt,
            rel_txt_meta,
            sub_img_wt,
            ocr_wt,
            rel_txt_wt * %(rel_txt_wt)s + 
            sub_img_wt * %(sub_img_wt)s + 
            ocr_wt * %(ocr_wt)s 
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

    args = {
        'rel_txt_wt': REL_TXT_WEIGHT,
        'sub_img_wt': SUB_IMG_WEIGHT,
        'ocr_wt': OCR_WEIGHT,
        'post_id': pid,
        'skip': request.args.get('skip', 0),
        'amount': request.args.get('amount', 5)
    }

    cursor.execute(query, args)

    results = []
    for result in cursor.fetchall():
        pids = set([result['post1_id'], result['post2_id']])
        if (len(pids) == 1): continue
        pids -= set([pid])
        assert(len(pids) == 1)
        del result['post1_id']
        del result['post2_id']
        result['id'] = list(pids)[0] 
        results.append(result)

    cursor.close()
    cnx.close()

    return jsonify(results)

### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
