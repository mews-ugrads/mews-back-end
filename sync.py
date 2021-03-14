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
        return 1

    # Grab Last Sync and Format
    state = loadConfig(SYNC_CONFIG_FILEPATH)
    lastSync = state['lastSync']

    # Query Mews DB
    mewsCursor = mewsCnx.cursor()
    query = ("SELECT url, image_url, reposts, replies, likes, when_posted, "
    "related_text, ocr_text, when_scraped, original_img_dir, original_img_filename, pic_id FROM scraped_images WHERE when_scraped > %s LIMIT 1")
    mewsCursor.execute(query, (lastSync,))

    # Extract Variables
    (post_url, image_url, reposts, replies, likes, when_posted, related_text, ocr_text, when_scraped, image_directory, image_filename, scrape_id) = mewsCursor.fetchone()
    when_updated = when_scraped

    # Disconnect from Mews DB
    mewsCnx.close()

    # Grab Mews-App Config
    with open('config/mews-app.json') as f:
        mewsAppConfig = json.load(f)

    # Connect to Mews-App DB
    try:
        mewsAppCnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        print(err)
        return 1

    # Insert into Mews-App
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
            'scrape_id': scrape_id
            }
    mewsAppCursor = mewsAppCnx.cursor()
    query = ("INSERT INTO Posts "
    "(post_url, image_url, reposts, replies, likes, when_posted, "
    "related_text, ocr_text, when_scraped, when_updated, image_directory, image_filename, scrape_id) VALUES "
    "(%(post_url)s, %(image_url)s, %(reposts)s, %(replies)s, %(likes)s, %(when_posted)s, %(related_text)s, "
    "%(ocr_text)s, %(when_scraped)s, %(when_updated)s, %(image_directory)s, %(image_filename)s, %(scrape_id)s)")

    try:
        mewsAppCursor.execute(query, data)
        mewsAppCnx.commit()
    except mysql.connector.Error as err:
        print(err)
        return 1

    mewsAppCnx.close()


### Main Execution

if __name__ == '__main__':
    syncImages()
