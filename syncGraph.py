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

### Functions

def loadConfig(filepath):
    with open(filepath) as f:
        return json.load(f)

def syncGraph():

    # Load in Text File

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

        (post_url, image_url, reposts, replies, likes, when_posted, related_text, ocr_text, when_scraped, image_directory, image_filename, scrape_id) = result
        when_updated = when_scraped

        # Insert into Mews-App
        # @FIXME: replace user_id to non-hard-coded
        data = {
                'post_url': post_url,
                'image_url': image_url,
                'reposts': reposts,
                'replies': replies,
                'likes': likes,
                'when_posted': when_posted,
                'related_text': related_text,
                'ocr_text': ocr_text,
                'when_scraped': when_scraped,
                'when_updated': when_updated,
                'image_directory': image_directory,
                'image_filename': image_filename,
                'scrape_id': scrape_id,
                'user_id': 1
                }
        mewsAppCursor = mewsAppCnx.cursor()
        query = ("INSERT INTO Posts "
        "(post_url, image_url, reposts, replies, likes, when_posted, "
        "related_text, ocr_text, when_scraped, when_updated, image_directory, image_filename, scrape_id, user_id) VALUES "
        "(%(post_url)s, %(image_url)s, %(reposts)s, %(replies)s, %(likes)s, %(when_posted)s, %(related_text)s, "
        "%(ocr_text)s, %(when_scraped)s, %(when_updated)s, %(image_directory)s, %(image_filename)s, %(scrape_id)s, %(user_id)s)")

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
