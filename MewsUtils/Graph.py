#!/usr/bin/env python3

from . import Posts

def getCentralGraph(upper_dt, lower_dt, skip, central_amount, rel_amount):

    # Initialize Return Structure
    output = { 'nodes': [], 'links': [] }

    # Call Central Posts
    posts, code = Posts.getCentralPosts(upper_dt, lower_dt, skip, central_amount)
    if code != 200:
        return posts, code

    # Add Central Posts to Graph
    for post in posts:
        post['central'] = True
        post['svg'] = post['image_url']
        output['nodes'].append(post)
        link = { 'source': post['id'], 'target': post['id'] }
        output['links'].append(link)

        # Call Related Posts
        relPosts, code = Posts.getRelatedPosts(post['id'], skip, rel_amount)
        if code != 200:
            return relPosts, code

        # Add Related Posts to Graph
        for neighbor in relPosts:
            neighbor['central'] = False
            output['nodes'].append(neighbor)
            link = { 'source': post['id'], 'target': neighbor['id'] }
            output['links'].append(link)

    return output, 200
