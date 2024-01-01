# external import
import logging
import pymongo
import asyncio, multiprocessing

# local import
from .logformat import LogFormatter
import vars.mongo, vars.settings

# logger
log = logging.getLogger("cc-capstone")
log_handler = logging.StreamHandler()
log_handler.setFormatter(LogFormatter())

log.addHandler(log_handler)
log.setLevel(logging.DEBUG)

log.info("Logger instantiated")

# mongo
mongo_client = pymongo.MongoClient(vars.mongo.mongo_uri)
db = mongo_client[vars.mongo.db]
[db[col] for col in vars.mongo.collections]

log.info("MongoDB instantiated")