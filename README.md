# mews-back-end
Backend for MEWS web application

## Description
XX

## Requirements
- This project uses Python Version 3.6.9
- All necessary Python libraries are recorded in requirements.txt

## Project Structure
- `config`: contains connection files and mySQL schema
- `data`: holds test data for use
- `app.py`: flask server file
- `syncPosts.py`: program to insert new posts from mews.scraped_images into mews_app.Posts
- `updatePosts.py`: program to update posts in mews_app.Posts from mews.scraped_images
- `syncGraph.py`: program to grab generated edge data and insert into mews_app.PostRelatedness and mews_app.PostCentrality

## To Install
This project uses a Python3.3+ virtual environment to install packages. Run the following to set up a virtual environment.
```console
$ python3 -m venv mews-venv 
```

Activate the virtual environment every time you work on this project
```console
$ source ./mews-venv/bin/activate
```

To install dependencies after activating the virtual environment:
```console
$ pip install -r requirements.txt
```

Deactivate to stop the virtual environment
```console
$ deactivate
```

## Run Server
Make sure you are in the MEWS virtual environment
```console
$ source ./mews-venv/bin/activate
```

Set the correct password for the mySQL connection configuration files: `config/mews-app.json`. Be sure to not commit the raw password.
```json
{
	"password": "INSERT_HERE"
}
```

To start up the server:
```console
$ ./app.py
```

Deactivate to stop the virtual environment
```console
$ deactivate
```

## Run Utility Programs
Make sure you are in the MEWS virtual environment
```console
$ source ./mews-venv/bin/activate
```

Set the correct password for the mySQL connection configuration files: `config/mews-app.json`. Be sure to not commit the raw password.
```json
{
	"password": "INSERT_HERE"
}
```

Run utility files in correct order
```console
-- First Step
$ ./syncPosts.py

-- Second Step
$ ./updatePosts.py

-- Third Step - Use -h flag to see available flags
$ ./syncGraph.py [FLAGS]
```

Deactivate to stop the virtual environment
```console
$ deactivate
```
