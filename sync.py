#!/usr/bin/env python3

### Imports

from datetime import datetime
import mysql.connector
import json
import os
import re
from tqdm import tqdm

### Constants

MEWS_CONFIG_FILEPATH = 'config/mews.json'
APP_CONFIG_FILEPATH = 'config/mews-app.json'
SYNC_CONFIG_FILEPATH = 'config/sync.json'

### Functions

def loadConfig(filepath):
    with open(filepath) as f:
        return json.load(f)

def connectSQL(config):
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.Error:
        return None

def getInsertedId(cursor):
    cursor.execute('SELECT LAST_INSERT_ID() as id;')
    row = cursor.fetchone()
    if row is None: 
        return None
    else:
        return row['id']

def getHashtagId(cursor, value):
    # Query Structure
    sql = '''
    SELECT 
        id
    FROM 
        Hashtags
    WHERE
        value = %(value)s
    LIMIT 1
    ;
    '''

    # Query Arguments
    args = {'value': value.lower()}

    # Run Query
    cursor.execute(sql, args)

    # Return Result
    row = cursor.fetchone()
    if row is None:
        return None
    else:
        return row['id']

def getPersonId(cursor, name):
    # Query Structure
    sql = '''
    SELECT FROM People
        id
    WHERE
        name = %(name)s
    LIMIT 1
    ;
    '''

    # Query Arguments
    args = {'name': name.lower()}

    # Run Query
    cursor.execute(sql, args)

    # Return Result
    row = cursor.fetchone()
    if row is None:
        return None
    else:
        return row['id']

def getUserId(cursor, platform, username):
    # Query Structure
    sql = '''
    SELECT 
        id
    FROM 
        Users
    WHERE
        platform = %(platform)s
        and 
        username = %(username)s
    LIMIT 1
    ;
    '''

    # Query Arguments
    args = {
        'platform': platform.lower(),
        'username': username
    }

    # Run Query
    cursor.execute(sql, args)

    # Return Result
    row = cursor.fetchone()
    if row is None:
        return None
    else:
        return row['id']

def insertHashtag(cursor, value):
    value = value.lower()

    # Check for Duplicate
    id = getHashtagId(cursor, value)
    if id is not None:
        return id

    # Query Structure
    sql = '''
    INSERT INTO Hashtags
        (value)
    VALUES
        (%(value)s)
    ON DUPLICATE KEY UPDATE value=value
    ;
    '''

    # Query Arguments
    args = {'value': value}
    
    # Run Query
    cursor.execute(sql, args)
    
    # Get ID of Newly Inserted Row
    return getInsertedId(cursor)

def insertHashtagInPost(cursor, hashtag_id, post_id):
    # Query Structure
    sql = '''
    INSERT INTO HashtagsInPosts
        (hashtag_id, post_id)
    VALUES
        (%(hashtag_id)s, %(post_id)s)
    ON DUPLICATE KEY UPDATE hashtag_id=hashtag_id
    ;
    '''

    # Query Arguments
    args = {'hashtag_id': hashtag_id, 'post_id': post_id}

    # Run Query
    cursor.execute(sql, args)    

def insertPerson(cursor, name):
    name = name.lower()

    # Check for Duplicate
    id = getPersonId(cursor, name)
    if id is not None:
        return id

    # Query Structure
    sql = '''
    INSERT INTO People
        (name)
    VALUES
        (%(name)s)
    ON DUPLICATE KEY UPDATE name=name
    ;
    '''

    # Query Arguments
    args = {'name': name}
    
    # Run Query
    cursor.execute(sql, args)
    
    # Get ID of Newly Inserted Row
    return getInsertedId(cursor)

def insertPersonInPost(cursor, person_id, post_id):
    # Query Structure
    sql = '''
    INSERT INTO PeopleInPosts
        (person_id, post_id)
    VALUES
        (%(person_id)s, %(post_id)s)
    ON DUPLICATE KEY UPDATE person_id=person_id
    ;
    '''

    # Query Arguments
    args = {'person_id': person_id, 'post_id': post_id}

    # Run Query
    cursor.execute(sql, args)    

def insertPostRelatedness(cursor, post1_id, post2_id, weight):
    # Query Structure
    sql = '''
    INSERT INTO PostRelatedness
        (post1_id, post2_id, weight)
    VALUES
        (%(post1_id)s, %(post2_id)s, %(weight)s)
    ON DUPLICATE KEY UPDATE weight=%(weight)s
    ;
    '''

    # Query Arguments
    args = {
        'post1_id': post1_id, 
        'post2_id': post2_id, 
        'weight': weight
    }

    # Run Query
    cursor.execute(sql, args)  

def insertPost(cursor, srcPost):
    srcPost['user_id'] = insertUser(cursor, srcPost['platform'], srcPost['username'])

    columns = [
        'user_id',
        'post_url', 
        'image_url', 
        'reposts',
        'replies',
        'likes', 
        'when_posted',
        'when_scraped',
        'when_updated',
        'related_text',
        'ocr_text',
        'image_directory',
        'image_filename',
        'scrape_id'
    ]

    # Query Structure
    sql = f'''
    INSERT INTO Posts (
        {','.join(columns)}
    )
    VALUES
    (
        {','.join(f'%({column})s' for column in columns)}  
    )
    ON DUPLICATE KEY UPDATE user_id=user_id
    ;
    '''

    # Query Arguments
    args = {column: srcPost.get(column) for column in columns} # missing arguments are inserted as NULL (None)

    # Run Query
    cursor.execute(sql, args)
    cursor.execute('SELECT id FROM Posts WHERE scrape_id=%(scrape_id)s LIMIT 1;', {'scrape_id': args['scrape_id']})
    post_id = cursor.fetchone()['id']

    # Insert Hashtags
    for hashtag in srcPost['hashtags']:
        if hashtag == None or hashtag == '': continue
        hashtag_id = insertHashtag(cursor, hashtag)
        insertHashtagInPost(cursor, hashtag_id, post_id)

    return post_id

def insertPostInCluster(cursor, post_id, cluster_id):
    # Query Structure
    sql = '''
    INSERT INTO PostsInClusters
        (post_id, cluster_id)
    VALUES
        (%(post_id)s, %(cluster_id)s
    ON DUPLICATE KEY UPDATE post_id=post_id
    ;
    '''

    # Query Arguments
    args = {
        'post_id': post_id, 
        'cluster_id': cluster_id
    }

    # Run Query
    cursor.execute(sql, args)  

def insertUser(cursor, platform, username):
    id = getUserId(cursor, platform, username)
    if id is not None:
        return id

    # Query Structure
    sql = '''
    INSERT INTO Users
        (platform, username)
    VALUES
        (%(platform)s, %(username)s)
    ON DUPLICATE KEY UPDATE platform=platform
    ;
    '''

    # Query Arguments
    args = {
        'platform': platform.lower(),
        'username': username
    }

    # Run Query
    cursor.execute(sql, args)

    return getInsertedId(cursor)

def pullPosts(cursor, after):
    # Query Structure
    sql = '''
    (
        SELECT
            url,
            image_url,
            reposts,
            replies,
            likes,
            when_posted,
            when_scraped,
            when_scraped2,
            related_text,
            ocr_text,
            original_img_dir,
            original_img_filename,
            pic_id,
            hashtags,
            platform,
            platform_username
        FROM
            scraped_images
        WHERE
            when_scraped > %(after)s
    )
    '''

    # Query Arguments
    args = {
        'after': after
    }

    # Run Query
    cursor.execute(sql, args)

    # Fetch Post Data
    post = cursor.fetchone()
    while post is not None:
        post = cursor.fetchone()
         # Transform Post Data
        results = {
            'post_url': post['url'], 
            'image_url': post['image_url'], 
            'reposts': post['reposts'],
            'replies': post['replies'],
            'likes': post['likes'], 
            'when_posted': post['when_posted'],
            'when_scraped': post['when_scraped'],
            'when_updated': post['when_scraped2'],
            'related_text': post['related_text'],
            'ocr_text': post['ocr_text'],
            'image_directory': post['original_img_dir'],
            'image_filename': post['original_img_filename'],
            'scrape_id': post['pic_id'],
            'hashtags': set(re.split(r',| |, |\|', post['hashtags'])),
            'platform': post['platform'],
            'username': post['platform_username']
        } 

        yield results

def syncImages():
    # Connect to Mews DB
    mewsConfig = loadConfig(MEWS_CONFIG_FILEPATH)
    mewsCnx = connectSQL(mewsConfig)

    # Connect to MewsApp DB
    appConfig = loadConfig(APP_CONFIG_FILEPATH)
    appCnx = connectSQL(appConfig)

    # Grab Last Sync and Format
    state = loadConfig(SYNC_CONFIG_FILEPATH)
    lastSync = state['lastSync']
    state['lastSync'] = str(datetime.now())

    # Pull Info
    appCursor = appCnx.cursor(dictionary=True)
    mewsCursor = mewsCnx.cursor(dictionary=True)
    try:
        posts = pullPosts(mewsCursor, lastSync)
        for post in tqdm(posts, leave=False):
            insertPost(appCursor, post)
            appCnx.commit()
    except:
        mewsCnx.close()
        appCnx.close()


    mewsCnx.close()
    appCnx.close()

    with open(SYNC_CONFIG_FILEPATH, 'w') as f:
        json.dump(state, f)

### Main Execution

if __name__ == '__main__':
    syncImages()
