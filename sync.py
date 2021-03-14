#!/usr/bin/env python3

### Imports

from datetime import datetime
import mysql.connector
import json
import os

### Functions

def syncImages():
    # Grab Mews Config
    with open('config/mews.json') as f:
        mewsConfig = json.load(f)

    # Connect to Mews DB
    try:
        mewsCnx = mysql.connector.connect(**mewsConfig)
    except mysql.connector.Error as err:
        return jsonify(error='Could Not Connect to Database'), 404

    # Grab Last Sync and Format
    with open('config/sync.json') as f:
        state = json.load(f)
    lastSync = state['lastSync']

    # Query Mews DB
    mewsCursor = mewsCnx.cursor()
    query = ("SELECT pic_id, original_img_filename, when_scraped FROM scraped_images WHERE when_scraped > %s")
    mewsCursor.execute(query, (lastSync,))

    # Print Info
    for (pic_id, original_img_filename, when_scraped) in mewsCursor:
        print("{}, {} was scraped on {}".format(pic_id, original_img_filename, when_scraped))


    # Grab Mews-App Config
    with open('config/mews-app.json') as f:
        mewsAppConfig = json.load(f)

    # Connect to Mews-App DB
    try:
        mewsAppCnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        return jsonify(error='Could Not Connect to Database'), 404

    # 
    mewsCursor = mewsCnx.cursor()
    query = ("INSERT INTO Posts pic_id, original_img_filename, when_scraped FROM scraped_images WHERE when_scraped > %s")

    mewsCnx.close()
    return "DATA!"


### Main Execution

if __name__ == '__main__':
    syncImages()
