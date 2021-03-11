#!/usr/bin/env python3

### Imports

from flask import Flask
import mysql.connector
import json
import os


### Globals

app = Flask(__name__)
os.environ['FLASK_ENV'] = 'development'


### API Routes

@app.route("/")
def test():
    # Grab Config JSON
    with open('config/mews-app.json') as f:
        mewsAppConfig = json.load(f)

    # Connect to DB
    try:
        cnx = mysql.connector.connect(**mewsAppConfig)
    except mysql.connector.Error as err:
        return "FAILED TO CONNECT!"
    else:
        cnx.close()
        return "CONNECTED TO DB"


### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
