# starting the logger module
import sys, os
sys.path.append('..')

import startup

import vars.mongo, vars.commoncrawl, vars.settings

import logging
import pymongo
import motor.motor_asyncio
import asyncio
import multiprocessing, threading, queue as qlib
import time

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

def consumer_thread(
        id: str,
        queue: multiprocessing.Queue,
        thread_queue: qlib.Queue,
        kill: threading.Event
):
    while True:
        if kill.is_set():
            log.warning(f"{id} recieved kill signal.")
            break

        if thread_queue.empty():
            log.warning(f"Thread Queue is empty! ({id})")

            time.sleep(vars.settings.wait_tick_speed)
            continue

        url = thread_queue.get()
        # log.debug(url)

        warcs = col_warc.find({
            "filename": url,
            "status": "not_processed"
        }).sort("offset", pymongo.ASCENDING)

        ranges = []
        m_ids = []

        for i in warcs:
            ranges.append(i["range"])
            m_ids.append(i["_id"])

        n_records = len(ranges)

        warc_item = {
            "range": ", ".join(ranges),
            "m_ids": m_ids,
            "url": vars.commoncrawl.base_url + url,
            "filename": url.split("/")[-1],
            "n_records": n_records
        }

        # log.debug(f"Consumer thread ({id}) recieved {warc_item['n_records']} warcs")
        thread_queue.task_done()
        queue.put(warc_item)

async def main(
        id: str,
        queue: multiprocessing.Queue,
        kill: threading.Event
):
    log.info(f"{id} manager started!")

    thread_queue = qlib.Queue()
    thread_kill = threading.Event()

    threads = []

    for i in range(vars.settings.warc["manager"]["threads"]):
        tmp_id = f"M-Thread-{i}"

        t = threading.Thread(
            target=consumer_thread,
            args=(
                tmp_id,
                queue,
                thread_queue,
                thread_kill
            )
        )

        t.start()
        threads.append(t)

    # start threads

    while True:
        if kill.is_set():
            thread_kill.set()
            log.warning(f"{id} killing threads...")

            while True:
                if all([not t.is_alive() for t in threads]):
                    log.warning(f"{id} all threads killed!")
                    
                    break
                else:
                    log.warning(f"{id} waiting for threads to die...")
                    time.sleep(vars.settings.wait_tick_speed)
            
            return
            

        log.debug("Fetching...")
        warc_urls = async_col_warc.aggregate([
            {
                "$match": {
                    "status": "not_processed"
                }
            },
            {
                "$group": {
                    "_id": "$filename"
                }
            },
            {
                "$limit": vars.settings.warc["query_limit"]
            }
        ])

        async for url in warc_urls:
            thread_queue.put(url["_id"])
        
        log.info(f"Thread Queue size: {thread_queue.qsize()}")
    

