#!/usr/bin/env python3

### Imports

from datetime import datetime
import mysql.connector
import json
import os

### Constants

MEWS_CONFIG_FILEPATH = 'config/mews.json'
MEWSAPP_CONFIG_FILEPATH = 'config/mews-app.json'
SYNC_CONFIG_FILEPATH = 'config/sync.json'
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


##
# @desc    converts txt file to graph
# --
# @param   fpath  file path for txt file; path has format "u;v;method;weight;x;x;x;"
# @return  g      graph in format of {v1: { e1: [w1, w2, w3, w4], e2: [...] }, v2: ...}
##
def load_txt(fpath):
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


def syncGraph(fpath):

    # Load in Text File
    g = load_txt(fpath)

    # Grab Mews-App Config
    mewsAppConfig = loadConfig(MEWSAPP_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        mewsAppCnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        print(err)
        return 1

    # Insert Edges into DB
    for source in g:
        for target in g[source]:

            data = {
                    'source': source,
                    'target': target,
                    'full_img_weight': g[source][target][FULL_IMG],
                    'rel_txt_weight': g[source][target][REL_TXT],
                    'sub_img_weight': g[source][target][SUB_IMG],
                    'ocr_weight': g[source][target][OCR]
                    }

            # Insert into Mews-App:PostRelatedness
            # @TODO: Insert real weights
            mewsAppCursor = mewsAppCnx.cursor()
            query = ("INSERT INTO PostRelatedness "
            "(post1_id, post2_id, weight) "
            "SELECT post1_id, post2_id, %(full_img_weight)s as weight "
            "FROM (SELECT id as post1_id FROM Posts WHERE image_filename LIKE '%(source)s.jpg') as id1 "
            "JOIN (SELECT id as post2_id FROM Posts WHERE image_filename LIKE '%(target)s.jpg') as id2;")

            try:
                mewsAppCursor.execute(query, data)
                mewsAppCnx.commit()
            except mysql.connector.Error as err:
                print(err)
                return 1

        # Disconnect from Mews-App
        mewsAppCnx.close()


### Main Execution

if __name__ == '__main__':
    syncGraph()
