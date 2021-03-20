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
UPDATE_CONFIG_FILEPATH = 'config/updatePosts.json'

### Functions

def loadConfig(filepath):
    with open(filepath) as f:
        return json.load(f)

def updatePosts():
    # Grab Mews Config
    mewsConfig = loadConfig(MEWS_CONFIG_FILEPATH)

    # Connect to Mews DB
    try:
        mewsCnx = mysql.connector.connect(**mewsConfig)
    except mysql.connector.Error as err:
        return 1

    # Grab Last Sync and Format
    state = loadConfig(UPDATE_CONFIG_FILEPATH)
    lastUpdate = state['lastUpdate']
    thisUpdateDatetime = str(datetime.now())

    # Query Mews DB
    mewsCursor = mewsCnx.cursor()
    query = ("SELECT pic_id, reposts, replies, likes, when_scraped2 FROM scraped_images WHERE when_scraped2 > %s")
    mewsCursor.execute(query, (lastUpdate,))

    # Grab Mews-App Config
    mewsAppConfig = loadConfig(MEWSAPP_CONFIG_FILEPATH)

    # Connect to Mews-App DB
    try:
        mewsAppCnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        print(err)
        return 1

    # Extract Variables
    for result in mewsCursor.fetchall():

        (scrape_id, reposts, replies, likes, when_updated) = result

        # Insert into Mews-App
        data = {
                'reposts': reposts,
                'replies': replies,
                'likes': likes,
                'when_updated': when_updated,
                'scrape_id': scrape_id,
                }
        mewsAppCursor = mewsAppCnx.cursor()
        query = ("UPDATE Posts "
        "SET reposts = %(reposts)s, replies = %(replies)s, likes = %(replies)s, when_updated = %(when_updated)s "
        "WHERE scrape_id = %(scrape_id)s;")

        try:
            mewsAppCursor.execute(query, data)
            mewsAppCnx.commit()
        except mysql.connector.Error as err:
            print(err)
            return 1

    # Disconnect from Mews and Mews-App
    mewsCnx.close()
    mewsAppCnx.close()

    # Update config file
    state['lastUpdate'] = thisUpdateDatetime
    with open(UPDATE_CONFIG_FILEPATH, 'w') as f:
        json.dump(state, f)



### Main Execution

if __name__ == '__main__':
    updatePosts()
