#!/usr/bin/env python3

import json

DB_CONFIG = {
  'user': 'mews_app_user',
  'password': '',
  'host': '127.0.0.1',
  'raise_on_warnings': True,
  'charset': 'utf8mb4',
  'collation': 'utf8mb4_general_ci'
}

def loadConfig(filepath):
    """
    @desc    Grabs JSON from file
    @return  JSON from filepath
    """
    with open(filepath) as f:
        return json.load(f)
