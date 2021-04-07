#!/usr/bin/env python3

### Imports

from datetime import datetime
import mysql.connector
import json
import sys
import os

### Constants

MEWS_CONFIG_FILEPATH = 'config/inter-mews.json'

### Functions

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


def updatePosts():
    '''
    @desc  updates Posts in mews_app if the when_scraped2 value in ...
           ... mews.scraped_images is greater than mews.when_updated
    '''

    # Grab Mews Config
    mewsConfig = loadConfig(MEWS_CONFIG_FILEPATH)
    mewsCnx = connectSQL(mewsConfig)
    mewsCursor = mewsCnx.cursor()

    # Query Mews DB For Updated Posts
    # Note: order needs to be same for Insert
    sql = '''
    SELECT
        reposts,
        replies,
        likes,
        when_scraped2,
        pic_id
    FROM
        mews.scraped_images AS src_post
    WHERE
        src_post.when_scraped2 > (SELECT when_updated FROM mews_app.Posts WHERE src_post.pic_id = scrape_id)
    ;
    '''
    mewsCursor.execute(sql)


    # Print and Exit if Needed
    updatedInfo = mewsCursor.fetchall()
    print(f'Selected {len(updatedInfo)} rows in mews:scraped_images')
    if len(updatedInfo) == 0:
        print('No rows need updating')
        sys.exit(0)

    # Insert into Mews-App
    sql = '''
    UPDATE mews_app.Posts
    SET
        reposts = %s,
        replies = %s,
        likes = %s,
        when_updated = %s
    WHERE scrape_id = %s
    ;
    '''

    try:
        mewsCursor.executemany(sql, updatedInfo)
    except mysql.connector.Error as err:
        print(err)

    mewsCnx.commit()

    # Disconnect from Mews and Mews-App
    print('Finished Updating')
    mewsCnx.close()


### Main Execution

if __name__ == '__main__':
    updatePosts()
