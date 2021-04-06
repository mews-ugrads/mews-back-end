#!/usr/bin/env python3

### Imports

from datetime import datetime
from collections import defaultdict
import mysql.connector
import json
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


def load_txt(fpath):
    '''
    @desc    converts txt file to graph
    --
    @param   fpath    file path for txt file; path has format "u;v;method;weight;x;x;x;"
    @return  weights  graph with weights
    @return  meta     graph with metadata
    '''

    vprint(f'Loading in graph from "{fpath}"')

    # Initialize Graph: g is dict of source dicts of target dicts
    weights    = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    meta       = defaultdict(lambda: defaultdict(dict))
    centrality = {}

    # Open File
    try:
        f = open(fpath, 'r')
    except:
        print(f'Cannot open "{fpath}"')
        sys.exit(1)

    # Ignore First Line
    next(f)

    # Loop Through Lines
    for line in f:
        # Grab Items
        items = line.rstrip().split(';')
        
        # Grab Attributes
        source     = items[2]
        source_img = items[0]
        target     = items[3]
        target_img = items[1]
        weight = float(items[4])
        method = items[5]

        # Grab Centrality Scores
        # @FIXME: Need to split because of how Pam separated by columns. Will be fixed to semi-colons when she fixes.
        scores = items[9].split(',')
        source_score = float(scores[0])
        target_score = float(scores[1])

        # Insert Centrality Scores
        centrality[source] = source_score
        centrality[target] = target_score

        # Grab Metadata and Insert into Graphs
        # Use of `eval` is for loading in Python sets
        if method == "related_text":
            data1 = eval(items[7])
            data2 = eval(items[8])
            weights[source][target]["r"] += weight
            meta[source][target]["r1"] = data1
            meta[source][target]["r2"] = data2

        if method == "subimage":
            data = items[6]
            weights[source][target]["s"] += weight
            meta[source][target]["s"] = data

        if method == "ocr":
            data1 = eval(items[7])
            data2 = eval(items[8])
            weights[source][target]["o"] += weight
            meta[source][target]["o1"] = data1
            meta[source][target]["o2"] = data2

    vprint('Finished Loading in Graph')
    f.close()

    return weights, meta, centrality

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
    weights, meta, centrality = load_txt(fpath)
    g = weights

    # Connect to Mews-App
    appConfig = loadConfig(APP_CONFIG_FILEPATH)
    appCnx = connectSQL(appConfig)
    appCursor = appCnx.cursor(dictionary=True)

    # Insert Edges into DB
    for source in g:
        for target in g[source]:

            # Grab Weights and Metadata
            rw = weights[source][target]['r']
            sw = weights[source][target]['s']
            ow = weights[source][target]['o']

            # Grab Metadata Sets, Perform Intersection to find Common, then Join by ';' to minimize length
            rm = None
            if rw:
                rm = meta[source][target]['r1'].intersection(meta[source][target]['r2'])
                rm = ';'.join(rm)
            om = None
            if ow:
                om = meta[source][target]['o1'].intersection(meta[source][target]['o2'])
                om = ';'.join(om)

            # Grab Metadata List as String
            sm = None
            if sw:
                sm = meta[source][target]['s']

            # Insert into Post Relatedness
            try:
                insertPostRelatedness(appCursor, source, target, rw, rm, sw, sm, ow, om)
            except Exception as ex:
                print(ex)
                continue

            appCnx.commit()

    # Insert Post Centrality into DB
    for post in centrality:

            # Grab Centrality Scores
            post_score = centrality[post]

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
    syncGraph('./data/april_2.txt')
