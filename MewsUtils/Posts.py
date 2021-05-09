#!/usr/bin/env python3


### Imports

import json
import os
import mysql.connector
from datetime import datetime, timedelta
import dateutil.parser as dt
from . import Connection, Images


### Functions

def getTrendingPosts(upper, lower, skip, amount, getBoxes, searchTerm=None):
    # Check Arguments
    try:
        assert(skip >= 0)
        assert(amount >= 0)
        upper_dt = dt.parse(upper)
        lower_dt = dt.parse(lower)
    except:
        return {'error': 'Invalid argument(s).'}, 400

    # Define Equation
    trendingEquation ='(LOG(reposts + 1) + LOG(replies + 1) + LOG(likes + 1) / 2 - DATEDIFF(CURDATE(), when_posted))'

    # Connect to DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor(dictionary=True)

    # Create Query
    sql = f'''
    SELECT
        Posts.id as id, 
        post_url, 
        reposts,
        replies, 
        likes,
        when_posted, 
        user_id,
        related_text, 
        ocr_text,
        when_scraped, 
        when_updated,
        platform,
        username,
        {trendingEquation} as score
    FROM
        mews_app.Posts,
        mews_app.Users
    WHERE
        when_posted BETWEEN %(lower_dt)s AND %(upper_dt)s 
        AND
        Posts.user_id = Users.id
        AND 
        (
            %(search_disabled)s 
            OR 
            related_text LIKE CONCAT('%', %(search_term)s, '%') 
            OR 
            ocr_text LIKE CONCAT('%', %(search_term)s, '%')
        )
    ORDER BY
        {trendingEquation} DESC
    LIMIT 
        %(skip)s, %(amount)s
    ;
    '''
    args = {
        'lower_dt': lower_dt,
        'upper_dt': upper_dt,
        'trendingEquation': trendingEquation,
        'skip': skip,
        'amount': amount,
        'search_disabled': searchTerm is None,
        'search_term': searchTerm if searchTerm else ''
    }

    cursor.execute(sql, args)

    # Extract Information
    trendingPosts = []
    for post in cursor.fetchall():
        post['image_url'] = Images.getImageURL(post['id'])
        post['heatmap_url'] = Images.getHeatmapURL(post['id'])
        trendingPosts.append(post)

    # Get Boxes for Each Post
    if getBoxes is True:
        for post in trendingPosts:

            sql = '''
            SELECT 
                sub_img_meta as coords,
                IF(post1_id=%(pid)s, post2_id, post1_id) as other_post_id
            FROM 
                mews_app.PostRelatedness
            WHERE
                (
                    post1_id = %(pid)s
                    OR 
                    post2_id = %(pid)s
                )
                AND
                sub_img_meta IS NOT NULL
            ;
            '''
            args = { 'pid': post['id'] }
            cursor.execute(sql, args)

            boxes = []
            for box in cursor.fetchall():
                boxes.append(box)

            post['boxes'] = boxes

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
    cursor = cnx.cursor(dictionary=True)

    # Format Query
    sql = '''
    SELECT
        Posts.id as id, 
        post_url, 
        reposts,
        replies, 
        likes,
        when_posted, 
        user_id,
        related_text, 
        ocr_text,
        when_scraped, 
        when_updated,
        platform,
        username
    FROM 
        mews_app.Posts,
        mews_app.Users
    WHERE 
        Posts.id = %(pid)s
        AND
        Posts.user_id = Users.id
    ;
    '''
    args = { 'pid': pid }

    # Query DB
    cursor.execute(sql, args)

    # Extract Information
    post = cursor.fetchone()
    if post is None:
        return {'error': 'Could not execute'}, 400

    post['image_url'] = Images.getImageURL(post['id'])
    post['heatmap_url'] = Images.getHeatmapURL(post['id'])

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
            A.id as id, 
            A.image_url,
            A.post_url, 
            A.reposts,
            A.replies, 
            A.likes,
            A.when_posted, 
            A.user_id,
            A.related_text, 
            A.ocr_text,
            A.when_scraped, 
            A.when_updated,
            B.rel_txt_wt,
            B.rel_txt_meta,
            B.ocr_meta,
            B.sub_img_wt,
            B.ocr_wt,
            B.scaled_sub_img_wt,
            B.total_wt,
            username,
            platform
        FROM 
            mews_app.Posts AS A,
            mews_app.Users,
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
            AND
            A.user_id = Users.id
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
            result['image_url'] = Images.getImageURL(pid)
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
    cursor = cnx.cursor(dictionary=True)

    # Create Query
    # Grabs 'amount' number of ordered central nodes within time frame, then grabs ...
    # ... their post information, then grabs corresponding user info
    sql = '''
    SELECT 
        post.id as id, 
        post.image_url, 
        post.post_url, 
        post.reposts, 
        post.replies, 
        post.likes, 
        post.when_posted, 
        post.score, 
        post.evaluated,
        username,
        platform
    FROM
        mews_app.Users,
        (SELECT
            id, 
            image_url, 
            post_url, 
            reposts, 
            replies, 
            likes, 
            when_posted, 
            user_id,
            score, 
            evaluated
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
    for post in cursor.fetchall():
        post['image_url'] = Images.getImageURL(post['id']),
        post['heatmap_url'] = Images.getHeatmapURL(post['id'])
        centralPosts.append(post)

    cnx.close()

    return centralPosts, 200
