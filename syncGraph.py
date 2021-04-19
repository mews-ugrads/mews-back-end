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

# Paths
MEWS_CONFIG_FILEPATH = 'config/inter-mews.json'
GRAPH_FOLDER = '/data/mews/images/mews_graph'
LOG_FOLDER = '/data/mews/images/mews_graph/log'

# Printing
SILENCE_STDOUT = False  # To output to stdout or not
PRESERVE_LOG = True     # To output to log or not
LOG_FP = None           # Log file pointer


### Functions

def usage(code):
    print(f'''Usage: {os.path.basename(sys.argv[0])} [-h -s -o LOGPATH]
    -h              Help message
    -s              Silence standard output
    -n              No log file
    -i  GRAPH_PATH  Input file path (default is file in {GRAPH_FOLDER}/)
    -o  LOG_PATH    Specify log file path (default generates file in {LOG_FOLDER}/)''')
    sys.exit(code)

def logprint(s):
    '''
    @desc   Prints on VERBOSE mode
    --
    @param  s  String to print
    '''
    global PRESERVE_LOG, SILENCE_STDOUT, LOG_FP

    if PRESERVE_LOG: print(s, file=LOG_FP)
    if not SILENCE_STDOUT: print(s)


def loadConfig(filepath):
    '''
    @desc   Loads the mysql config json files
    --
    @param  filepath  path to config file
    '''
    try:
        with open(filepath) as f:
            return json.load(f)
    except Exception as ex:
        logprint(str(ex))
        sys.exit(1)


def load_json(fpath):
    '''
    @desc    parses JSON graph file
    --
    @param   fpath    file path for json file
    @return  posts    dict with post data (centrality, ocr, rel_txt)
    @return  edges    dict with edge data
    '''

    logprint(f'Loading in graph from "{fpath}"')

    # Grab JSON
    try:
        f = open(fpath, mode='r')
        data = json.load(f)
    except Exception as ex:
        logprint(str(ex))
        sys.exit(1)

    try:
        posts = data['posts']
        edges = data['edges']
    except Exception as ex:
        logprint(str(ex))
        sys.exit(1)

    return posts, edges


def getSubimageWeightsMeta(edges, source, target):
    '''
    @desc  Returns subimage weight (float, else None) and metadata (string, else None)
    --
    @return  sw  represents subimage weights
    @return  sm  represents subimage metadata
    '''
    sub_edges = edges[source][target].get('subimage', [])
    labels = []
    sw = 0 if len(sub_edges) > 0 else None
    sm = None
    if len(sub_edges) > 0:
        for tup in sub_edges:
            sw += tup[0]
            labels.append(tup[1])
        sm = '|'.join(labels)
        if len(sm) == 0:
            sm = None
    return sw, sm


def getRelTxtWeightsMeta(posts, edges, source, target):
    '''
    @desc    Returns rel-text weight (float, else None) and metadata (string, else None)
    --
    @return  rw  represents related_text weights
    @return  rm  represents related_text metadata
    '''
    rw = edges[source][target].get('rel_text')
    rm = None
    if rw:
        rm = eval(posts[source].get('related_text', 'set()')).intersection(eval(posts[target].get('related_text', 'set()')))
        rm = '|'.join(rm)
        if len(rm) == 0:
            rm = None
    return rw, rm


def getOcrWeightsMeta(posts, edges, source, target):
    '''
    @desc  Returns ocr weight (float, else None) and metadata (string, else None)
    --
    @return  ow  represents ocr weights
    @return  om  represents ocr metadata
    '''
    ow = edges[source][target].get('ocr')
    om = None
    if ow:
        om = eval(posts[source].get('ocr', 'set()')).intersection(eval(posts[target].get('ocr', 'set()')))
        om = '|'.join(om)
        if len(om) == 0:
            om = None
    return ow, om


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
    INSERT INTO mews_app.PostRelatedness (
        post1_id,
        post2_id,
        rel_txt_wt, rel_txt_meta, 
        sub_img_wt, sub_img_meta, 
        ocr_wt, ocr_meta
    )
    VALUES (
        (SELECT id FROM mews_app.Posts WHERE scrape_id = %(source)s),
        (SELECT id FROM mews_app.Posts WHERE scrape_id = %(target)s),
        %(rw)s, %(rm)s, 
        %(sw)s, %(sm)s,
        %(ow)s, %(om)s
    )
    ;
    '''

    logprint(f'Inserting Source: {source}; Target: {target}; Weights (r-s-o): {rw}-{sw}-{ow}')
    logprint(f'  R Meta: {rm}')
    logprint(f'  S Meta: {sm}')
    logprint(f'  O Meta: {om}')

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
    INSERT INTO mews_app.PostCentrality (
        post_id,
        score,
        evaluated
    )
    VALUES (
        (SELECT id FROM mews_app.Posts WHERE scrape_id = %(pid)s),
        %(score)s,
        %(evaluated)s
    )
    ;
    '''
    logprint(f'Inserting Centrality: {pid}; Score: {score}; evaluated: {evaluated}')

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
    '''
    @desc  grabs JSON, inserts into PostRelatedness and PostCentrality
    '''

    # Load in Text File
    posts, edges = load_json(fpath)

    # Connect to Mews-App
    appConfig = loadConfig(MEWS_CONFIG_FILEPATH)
    try:
        appCnx = mysql.connector.connect(**appConfig)
    except mysql.connector.Error as err:
        logprint(str(err))
        sys.exit(0)

    appCursor = appCnx.cursor(dictionary=True)

    # Insert Edges into DB
    for source in edges:
        for target in edges[source]:

            # Grab Weights and Metadata
            rw, rm = getRelTxtWeightsMeta(posts, edges, source, target)
            ow, om = getOcrWeightsMeta(posts, edges, source, target)
            sw, sm = getSubimageWeightsMeta(edges, source, target)

            # Insert into Post Relatedness
            try:
                insertPostRelatedness(appCursor, source, target, rw, rm, sw, sm, ow, om)
            except Exception as ex:
                logprint(str(ex))
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
                logprint(str(ex))
                continue

            appCnx.commit()

    # Disconnect from Mews-App
    appCnx.close()


### Main Execution

if __name__ == '__main__':

    # Variables
    log_path = None
    graph_path = None

    # Parse Command Line
    args = sys.argv[1:]
    while len(args) and args[0].startswith('-') and len(args[0]) > 1:
        arg = args.pop(0)
        if arg == '-h':
            usage(0)
        elif arg == '-o':
            log_path = args.pop(0)
        elif arg == '-i':
            graph_path = args.pop(0)
        elif arg == '-s':
            SILENCE_STDOUT = True
        elif arg == '-n':
            PRESERVE_LOG = False
        else:
            usage(1)

    # Grab Today's Date
    today = datetime.today().strftime('%Y_%-m_%-d')

    # Initialize Log File
    if log_path is None and PRESERVE_LOG is True:
        log_path = LOG_FOLDER + '/syncGraph_' + str(today) + '.log'
        try:
            LOG_FP = open(log_path, mode='w')
        except Exception as ex:
            print(str(ex))
            sys.exit(1)

    # Print Log File
    if PRESERVE_LOG is True:
        logprint(f'Initialized Log File: {log_path}')

    # Grab Today's Graph File
    if graph_path is None:
        graph_path = GRAPH_FOLDER + '/edges_data_' + str(today) + '.json'
    logprint(f'Input Graph File: {graph_path}')

    # Sync Graph to
    syncGraph(graph_path)

    # Exit
    sys.exit(0)
