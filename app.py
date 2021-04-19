#!/usr/bin/env python3

### Imports

from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import mysql.connector
import json
import os
from flask_cors import CORS, cross_origin
from MewsUtils import Posts


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
        amount = int(request.args.get('amount', 3))
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
    @param   skip   - number of posts to skip (int)
    @param   amount - number of posts to return (int)
    @param   lower  - lower bound for when_posted (datetime syntax)
    @param   upper  - upper bound for when_posted (datetime syntax)
    --
    @return  list of central posts and related posts with links
    """
    # Grab Mews-App Config
    config = loadConfig(DB_CONFIG_FILEPATH)

    # Get Request Arguments
    upper_dt = request.args.get('upper', type=datetime, default = datetime.now())
    lower_dt = request.args.get('lower', type=datetime, default = datetime.now() - timedelta(days=30))
    skip = request.args.get('skip', type=int, default=0)
    amount = request.args.get('amount', type=int, default=50)

    # Initialize Return Structure
    output = { 'nodes': [], 'links': [] }

    # Grab Central Posts
    posts = json.loads(getCentralPosts())
    for post in posts:
        post['central'] = True
        output['nodes'].append(post)
        link = { 'source': post['id'], 'target': post['id'] }
        output['links'].append(link)

        # Grab Related Posts
        relPosts = json.loads(getRelatedPosts(post['id']))
        for neighbor in relPosts:
            neighbor['central'] = False
            output['nodes'].append(neighbor)
            link = { 'source': post['id'], 'target': neighbor['id'] }
            output['links'].append(link)
    
    return jsonify(output), 200

    


### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
