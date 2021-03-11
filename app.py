#!/usr/bin/env python3

### Imports

from flask import Flask
import os


### Globals

app = Flask(__name__)
os.environ['FLASK_ENV'] = 'development'


### API Routes

@app.route("/")
def hello():
    return "Hello World!"


### Main Execution

if __name__ == "__main__":
    app.run(debug=True)
