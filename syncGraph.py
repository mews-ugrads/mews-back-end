#!/usr/bin/env python3

'''
' @file   syncGraph.py
' @desc   Grabs input JSON file of posts and edges, inserts into PostRelatedness ...
'         ... the edges and inserts into PostCentrality the scores from posts.
' @notes  The file has scrape_id's. For inserting into our DB, we need to search for our id's (seen below in queries).
'''

### Imports

from datetime import datetime
from collections import defaultdict
import mysql.connector
import json
import sys
import os


### Constants

APP_CONFIG_FILEPATH = 'config/mews-app.json'
SYNC_GRAPH_CONFIG_FILEPATH = 'config/syncGraph.json'
VERBOSE = True


### Functions

def vprint(s):
    '''
    @desc   Prints on VERBOSE mode
    --
    @param  s  String to print
    '''
    global VERBOSE
    if VERBOSE: print(s)


def loadConfig(filepath):
    '''
    @desc   Loads the mysql config json files
    --
    @param  filepath  path to config file
    '''
    with open(filepath) as f:
        return json.load(f)


def connectSQL(config):
    '''
    @desc   Connects to mysql
    --
    @param  config  mysql.connector config object
    '''
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.Error:
        raise
        return None


def load_json(fpath):
    '''
    @desc    parses JSON graph file
    --
    @param   fpath    file path for json file
    @return  posts    dict with post data (centrality, ocr, rel_txt)
    @return  edges    dict with edge data
    '''

    vprint(f'Loading in graph from "{fpath}"')

    # Grab JSON
    with open(fpath) as f:
        data = json.load(f)

    posts = data['posts']
    edges = data['edges']

    return posts, edges


def insertPostRelatedness(cursor, source, target, rw, rm, sw, sm, ow, om):
    '''
    @desc   Insert information into Post Relatedness
    --
    @param  cursor  cursor for mysql.connector
    @param  source  scrape_id of post 1
    @param  target  scrape_id of post 2
    @param  rw      related text weight
    @param  rm      related text metadata
    @param  sw      subimage weight
    @param  sm      subimage metadata
    @param  ow      ocr weight
    @param  om      ocr metadata
    '''

    # Query Structure
    sql = '''
    INSERT INTO PostRelatedness (
        post1_id,
        post2_id,
        rel_txt_wt, rel_txt_meta, 
        sub_img_wt, sub_img_meta, 
        ocr_wt, ocr_meta
    )
    VALUES (
        (SELECT id FROM Posts WHERE scrape_id = %(source)s),
        (SELECT id FROM Posts WHERE scrape_id = %(target)s),
        %(rw)s, %(rm)s, 
        %(sw)s, %(sm)s,
        %(ow)s, %(om)s
    )
    ;
    '''

    vprint(f'Inserting Source: {source}; Target: {target}; Weights (r-s-o): {rw}-{sw}-{ow}')
    vprint(f'  R Meta: {rm}')
    vprint(f'  S Meta: {sm}')
    vprint(f'  O Meta: {om}')

    # Query Arguments
    args = {
        'source': source,
        'target': target,
        'rw': rw,
        'rm': rm,
        'sw': sw,
        'sm': sm,
        'ow': ow,
        'om': om
    }

    # Run Query
    try:
        cursor.execute(sql, args)  
    except mysql.connector.Error:
        raise


def insertPostCentrality(cursor, pid, score):
    '''
    @desc   Insert information into Post Centrality
    --
    @param  cursor  cursor for mysql.connector
    @param  pid     scrape_id
    @param  score   centrality score
    '''

    # Grab Date
    evaluated = datetime.now()

    # Query Structure
    sql = '''
    INSERT INTO PostCentrality (
        post_id,
        score,
        evaluated
    )
    VALUES (
        (SELECT id FROM Posts WHERE scrape_id = %(pid)s),
        %(score)s,
        %(evaluated)s
    )
    ;
    '''
    vprint(f'Inserting Centrality: {pid}; Score: {score}; evaluated: {evaluated}')

    # Query Arguments
    args = {
        'pid': pid,
        'score': score,
        'evaluated': evaluated
    }

    # Run Query
    try:
        cursor.execute(sql, args)  
    except mysql.connector.Error:
        raise


def syncGraph(fpath):

    # Load in Text File
    posts, edges = load_txt(fpath)

    # Connect to Mews-App
    appConfig = loadConfig(APP_CONFIG_FILEPATH)
    appCnx = connectSQL(appConfig)
    appCursor = appCnx.cursor(dictionary=True)

    # Insert Edges into DB
    for source in edges:
        for target in edges[source]:

            # Grab Weights
            rw = edges[source][target].get('rw')
            sw = edges[source][target].get('sw')
            ow = edges[source][target].get('ow')

            # Grab Metadata Sets, Perform Intersection to find Common, then Join by ';' to minimize length
            rm = None
            om = None
            if rw:
                rm = eval(posts[source].get('related_text', '{}')).intersection(eval(posts[target].get('related_text', '{}')))
                rm = '|'.join(rm)
            if ow:
                om = eval(posts[source].get('ocr', 'set()')).intersection(eval(posts[target].get('ocr', 'set()')))
                om = '|'.join(om)

            # Grab Metadata List as String
            sm = None
            if sw:
                sm = edges[source][target].get('label')
            

            # Insert into Post Relatedness
            try:
                insertPostRelatedness(appCursor, source, target, rw, rm, sw, sm, ow, om)
            except Exception as ex:
                print(ex)
                continue

            appCnx.commit()

    # Insert Post Centrality into DB
    for post in posts:

            # Grab Centrality Scores
            post_score = posts[post]['score']

            # Insert Source into Post Centrality
            try:
                insertPostCentrality(appCursor, post, post_score)
            except Exception as ex:
                print(ex)
                continue

            appCnx.commit()

    # Disconnect from Mews-App
    appCnx.close()


### Main Execution

if __name__ == '__main__':
    # @TODO: Grab graph file
    syncGraph('./data/april_2.json')
