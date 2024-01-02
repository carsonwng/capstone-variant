# starting the logger module
import sys, os
sys.path.append('..')

import startup

import vars.mongo, vars.commoncrawl, vars.settings

import logging
import pymongo
import motor.motor_asyncio
import asyncio

# setup
log = logging.getLogger("cc-capstone")
mongo_client = pymongo.MongoClient(vars.mongo.mongo_uri)
async_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(vars.mongo.mongo_uri)

db = mongo_client[vars.mongo.db]
async_db = async_mongo_client[vars.mongo.db]

col_warc = db["warc"]
async_col_warc = async_db["warc"]

mongo_queue = vars.mongo.QueueMongo(
    async_col_warc,
    batch_size = vars.settings.batch_size
)

async def main():
    try:
        pass
        
        # read from warc collection
        # if warc collection is empty, wait for warc files to process
        # if warc collection is not empty, process warc files
    
        # filter warcs by status, group warc files by url, sort by offset
        # for each url, process warc files

        # fetch byte ranges per url
        # then write to gzipped file
        # removing boundaries
    
        # filter warc by JSON-LD
        # then check schema.org types
        # finally, if the WARC is valid and a Recipe type, update mongoDB with Schema.org

        # otherwise, update mongoDB with processed but no Schema.org

    except KeyboardInterrupt:
        log.warning("KeyboardInterrupt detected, closing...")