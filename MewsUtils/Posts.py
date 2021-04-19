#!/usr/bin/env python3

import json
import os
import mysql.connector
from datetime import datetime, timedelta
from . import Connection

DB_CONFIG_FILEPATH = 'config/inter-mews.json'

def getCentralPosts(upper_dt, lower_dt, skip, amount):

    # Check Arguments
    try:
        assert(skip >= 0)
        assert(amount >= 0)
    except:
        return {'error': 'Invalid argument(s).'}, 400

    # Grab Mews-App Config
    # config = Connection.loadConfig(DB_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor()

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

    return centralPosts, 200
