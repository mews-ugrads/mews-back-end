#!/usr/bin/env python3


### Imports

import json
import os
import mysql.connector
from flask import jsonify, send_file, abort
from . import Connection

### Functions

def getPostImage(pid):
    # Connect to DB
    try:
        cnx = mysql.connector.connect(**Connection.DB_CONFIG)
    except mysql.connector.Error as err:
        return {'error': 'Could not connect to DB'}, 400
    cursor = cnx.cursor(dictionary=True)

    # Query ID of Most Recent Clustering
    sql = '''
        SELECT
            CONCAT(Posts.image_directory, Posts.image_filename) as filepath
        FROM
            mews_app.Posts
        WHERE
            Posts.id = %(pid)s
        LIMIT 1
        ;
    '''

    args = { 'pid':pid }

    cursor.execute(sql, args)

    parent_dir = '/data/mews/'
    result = cursor.fetchone()
    if result is None:
        abort(404)
    filepath = parent_dir + result['filepath']

    return send_file(filepath, 'image/jpeg')