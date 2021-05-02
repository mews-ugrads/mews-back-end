#!/usr/bin/env python3


### Imports

import json
import os
import mysql.connector
from datetime import datetime, timedelta
import dateutil.parser as dt
from . import Connection


### Functions

def getTrendingPosts(upper, lower, skip, amount):
    # Check Arguments
    try:
        assert(skip >= 0)
        assert(amount >= 0)
        upper_dt = dt.parse(upper)
        lower_dt = dt.parse(lower)
    except:
        return {'error': 'Invalid argument(s).'}, 400

    # Define Equation
    trendingEquation ='(10 * reposts + 10 * replies + likes)'

    # Connect to DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor()

    # Create Query
    cursor = cnx.cursor()
    sql = '''
    SELECT
        id, image_url,
        post_url, reposts,
        replies, likes,
        when_posted, user_id,
        related_text, ocr_text,
        when_scraped, when_updated
    FROM
        mews_app.Posts
    WHERE
        when_posted BETWEEN %(lower_dt)s AND %(upper_dt)s 
    ORDER BY
        %(trendingEquation)s DESC
    LIMIT 
        %(skip)s, %(amount)s
    ;
    '''
    args = {
        'lower_dt': lower_dt,
        'upper_dt': upper_dt,
        'trendingEquation': trendingEquation,
        'skip': skip,
        'amount': amount
    }

    cursor.execute(sql, args)

    # Extract Information
    trendingPosts = []
    for result in cursor.fetchall():
        (post_id, image_url, post_url, reposts, replies, likes, when_posted, user_id, related_text, ocr_text, when_scraped, when_updated) = result
        post = {
            'id': post_id,
            'image_url': f'/posts/{post_id}/image',
            'post_url': post_url,
            'reposts': reposts,
            'replies': replies,
            'likes': likes,
            'when_posted': when_posted,
            'user_id': user_id,
            'related_text': related_text,
            'ocr_text': ocr_text,
            'when_scraped': when_scraped,
            'when_updated': when_updated
        }
        trendingPosts.append(post)

    cnx.close()

    return trendingPosts, 200


def getPost(pid):

    # Check PID
    try:
        pid = int(pid)
        assert(pid >= 0)
    except:
        return {'error': 'Invalid post id.'}, 400

    # Connect to DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor()

    # Format Query
    sql = '''
    SELECT
        id, image_url,
        post_url, reposts,
        replies, likes,
        when_posted, user_id,
        related_text, ocr_text,
        when_scraped, when_updated
    FROM mews_app.Posts 
    WHERE id = %(pid)s;
    ;
    '''
    args = { 'pid': pid }

    # Query DB
    cursor.execute(sql, args)

    # Extract Information
    result = cursor.fetchone()
    if result is None:
        return {'error': 'Could not execute'}, 400

    (post_id, image_url, post_url, reposts, replies, likes, when_posted, user_id, related_text, ocr_text, when_scraped, when_updated) = result
    post = {
            'id': post_id,
            'image_url': f'/posts/{post_id}/image',
            'post_url': post_url,
            'reposts': reposts,
            'replies': replies,
            'likes': likes,
            'when_posted': when_posted,
            'user_id': user_id,
            'related_text': related_text,
            'ocr_text': ocr_text,
            'when_scraped': when_scraped,
            'when_updated': when_updated
            }

    sql = '''
    SELECT DISTINCT sub_img_meta
    FROM mews_app.PostRelatedness
    WHERE post1_id = %(pid)s
    ;
    '''
    args = { 'pid': pid }

    # Query DB
    cursor.execute(sql, args)

    boxes = []
    for result in cursor.fetchall():
        (box,) = result
        boxes.append(box)

    post['boxes'] = boxes
        
    cnx.close()

    return post, 200

def getRelatedPosts(pid, skip, amount):

    # parse args
    try:
        pid = int(pid)
        assert(pid >= 0)
    except:
        return {'error': 'Invalid post id'}, 400

    try:
        assert(skip >= 0)
    except:
        return {'error': 'Invalid parameter `skip`'}, 400

    try:
        assert(amount >= 0)
    except:
        return {'error': 'Invalid parameter `amount`'}, 400

    # Connect to DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor()

    # Constants to be Tuned
    REL_TXT_WEIGHT = 1
    SUB_IMG_WEIGHT = 1
    OCR_WEIGHT = 1

    # Query Mews-App DB
    cursor = cnx.cursor(dictionary=True)
    query = '''
        SELECT
            A.id, A.image_url,
            A.post_url, A.reposts,
            A.replies, A.likes,
            A.when_posted, A.user_id,
            A.related_text, A.ocr_text,
            A.when_scraped, A.when_updated,
            B.rel_txt_wt,
            B.rel_txt_meta,
            B.ocr_meta,
            B.sub_img_wt,
            B.ocr_wt,
            B.scaled_sub_img_wt,
            B.total_wt
        FROM 
            mews_app.Posts AS A,
            (SELECT 
                post1_id,
                post2_id,
                IF(post1_id = %(post_id)s, post2_id, post1_id) AS rel_id,
                rel_txt_wt,
                rel_txt_meta,
                ocr_meta,
                sub_img_wt,
                ocr_wt,
                scaled_sub_img_wt,
                total_wt
            FROM 
                mews_app.PostRelatedness
            WHERE
                post1_id = %(post_id)s
                OR
                post2_id = %(post_id)s
            ORDER BY
                total_wt DESC
            LIMIT
                %(skip)s, %(amount)s 
            ) AS B
        WHERE
            A.id = B.rel_id
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
            result['image_url'] = f'/posts/{pid}/image'
            results.append(result)
        except:
            pass

    # Clean Up
    cursor.close()
    cnx.close()

    # Return Results
    return results, 200


def getCentralPosts(upper, lower, skip, amount):

    # Check Arguments
    try:
        assert(skip >= 0)
        assert(amount >= 0)
        upper_dt = dt.parse(upper)
        lower_dt = dt.parse(lower)
    except:
        return {'error': 'Invalid argument(s).'}, 400

    # Connect to DB
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
                'image_url': f'/posts/{post_id}/image',
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
