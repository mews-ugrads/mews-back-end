#!/usr/bin/env python3

### Imports

from datetime import datetime
import mysql.connector
import json
import os

### Constants

APP_CONFIG_FILEPATH = 'config/mews-app.json'
SYNC_GRAPH_CONFIG_FILEPATH = 'config/syncGraph.json'

NUM_METHODS = 4
FULL_IMG = 0
REL_TXT  = 1
SUB_IMG  = 2
OCR      = 3


### Functions

def loadConfig(filepath):
    with open(filepath) as f:
        return json.load(f)

def connectSQL(config):
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.Error:
        raise
        return None


def load_txt(fpath):
    '''
    @desc    converts txt file to graph
    --
    @param   fpath  file path for txt file; path has format "u;v;method;weight;x;x;x;"
    @return  g      graph in format of {v1: { e1: [w1, w2, w3, w4], e2: [...] }, v2: ...}
    '''

    # Initialize Graph
    g = {}

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
        # @TODO: Grab metadata and insert into graph
        source = items[0]
        target = items[1]
        method = items[2]
        weight = float(items[3])

        # Insert Vertices
        if source not in g: g[source] = {}
        if target not in g[source]: g[source][target] = NUM_METHODS * [0]

        # Add Weights
        if method == "full_image_query":
            g[source][target][FULL_IMG] += weight
        if method == "related_text":
            g[source][target][REL_TXT] += weight
        if method == "subimage":
            g[source][target][SUB_IMG] += weight
        if method == "ocr":
            g[source][target][OCR] += weight

    f.close()
    return g

def insertPostRelatedness(cursor, source, target, fw, fm, rw, rm, sw, sm, ow, om):
    '''
    @desc   Insert information into Post Relatedness
    --
    @param  cursor  cursor for mysql.connector
    @param  source  img filename of 1st post (without .jpg)
    @param  target  img filename of 2nd post (without .jpg)
    @param  fw      full image query weight
    @param  fm      full image query metadata
    @param  rw      related text weight
    @param  rm      related text metadata
    @param  sw      subimage weight
    @param  sm      subimage metadata
    @param  ow      ocr weight
    @param  om      ocr metadata
    '''

    # Query Structure
    sql = '''
    INSERT INTO PostRelatedness
        (post1_id, post2_id, full_img_wt, full_img_meta, rel_txt_wt, 
        rel_txt_meta, sub_img_wt, sub_img_meta, ocr_wt, ocr_meta)
    SELECT post1_id, post2_id, %(full_img_weight)s as weight, %(fw)s as full_img_wt, %(fm)s as full_img_meta,
    %(rw)s as rel_txt_wt, %(rm)s as rel_txt_meta, %(sw)s as sub_img_wt, %(sm)s as sub_img_meta,
    %(ow)s as ocr_wt, %(om)s as ocr_meta
        FROM (SELECT id as post1_id FROM Posts WHERE image_filename LIKE '%(source)s.jpg') as id1
        JOIN (SELECT id as post2_id FROM Posts WHERE image_filename LIKE '%(target)s.jpg') as id2
    ;
    '''
    # @TODO: Remove print
    print(f'Inserting {source}-{target}: {fw}-{rw}-{sw}-{ow}')

    # Query Arguments
    args = {
        'source': source,
        'target': target,
        'fw': fw,
        'fm': fm,
        'rw': rw,
        'rm': rm,
        'sw': sw,
        'sm': sm,
        'ow': ow,
        'om': om
    }

    # Run Query
    cursor.execute(sql, args)  


def syncGraph(fpath):

    # Load in Text File
    g = load_txt(fpath)

    # Connect to Mews-App
    appConfig = loadConfig(APP_CONFIG_FILEPATH)
    appCnx = connectSQL(appConfig)
    appCursor = appCnx.cursor(dictionary=True)

    # Insert Edges into DB
    for source in g:
        for target in g[source]:

            # Grab Weights and Metadata
            fw = g[source][target][FULL_IMG]
            fm = ''
            rw = g[source][target][REL_TXT]
            rm = ''
            sw = g[source][target][SUB_IMG]
            sm = ''
            ow = g[source][target][OCR]
            om = ''

            # Insert into Post Relatedness
            insertPostRelatedness(appCursor, source, target, fw, fm, rw, rm, sw, sm, ow, om)
            appCnx.commit()

    # Disconnect from Mews-App
    appCnx.close()


### Main Execution

if __name__ == '__main__':
    syncGraph()
