#!/usr/bin/env python3

from . import Posts

def getCentralGraph(upper, lower, skip, central_amount, rel_amount):

    # Initialize Return Structure
    output = { 'nodes': [], 'links': [] }

    # Call Central Posts
    posts, code = Posts.getCentralPosts(upper, lower, skip, central_amount)
    if code != 200:
        return posts, code

    # Add Central Posts to Graph
    central = set()
    for post in posts:
        post['central'] = True
        post['svg'] = f'/posts/{post["id"]}/image'
        output['nodes'].append(post)
        link = { 'source': post['id'], 'target': post['id'] }
        output['links'].append(link)
        central.add(post['id'])

        # Call Related Posts
        relPosts, code = Posts.getRelatedPosts(post['id'], skip, rel_amount)
        if code != 200:
            return relPosts, code

        # Add Related Posts to Graph
        # Note: We can add the link even if both nodes are central, but we don't want to ...
        # ... submit multiple instances of same node with different `central` attribute
        for neighbor in relPosts:
            link = { 'source': post['id'], 'target': neighbor['id'] }
            output['links'].append(link)
            if neighbor['id'] in central: continue
            neighbor['central'] = False
            output['nodes'].append(neighbor)

    return output, 200
