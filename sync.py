#!/usr/bin/env python3

### Imports

from datetime import datetime
import mysql.connector
import json
import os

### Constants

MEWS_CONFIG_FILEPATH = 'config/mews.json'
SYNC_CONFIG_FILEPATH = 'config/sync.json'

### Functions

def loadConfig(filepath):
    with open(filepath) as f:
        return json.load(f)

def syncImages():
    # Grab Mews Config
    mewsConfig = loadConfig(MEWS_CONFIG_FILEPATH)

    # Connect to Mews DB
    try:
        mewsCnx = mysql.connector.connect(**mewsConfig)
    except mysql.connector.Error as err:
        return jsonify(error='Could Not Connect to Database'), 404

    # Grab Last Sync and Format
    state = loadConfig(SYNC_CONFIG_FILEPATH)
    lastSync = state['lastSync']

    # Query Mews DB
    mewsCursor = mewsCnx.cursor()
    query = ("SELECT pic_id, original_img_filename, when_scraped FROM scraped_images WHERE when_scraped > %s")
    mewsCursor.execute(query, (lastSync,))

    # Print Info
    for (pic_id, original_img_filename, when_scraped) in mewsCursor:
        print("{}, {} was scraped on {}".format(pic_id, original_img_filename, when_scraped))

    mewsCnx.close()
    return "DATA!"


### Main Execution

if __name__ == '__main__':
    syncImages()
