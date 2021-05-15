#! /bin/bash
./mews-venv/bin/python ./syncPosts.py
./mews-venv/bin/python ./syncGraph.py -n -s
./mews-venv/bin/python ./clusterPosts.py --daily `date --date="yesterday" +"%Y-%m-%d"`