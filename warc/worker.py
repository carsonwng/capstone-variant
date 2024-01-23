# TODO: REWRITE FOR WORKER

# starting the logger module
import sys, os
sys.path.append('..')

import startup

import vars.mongo, vars.commoncrawl, vars.settings
from handlers.default import DefaultHandler

import logging
import pymongo
import motor.motor_asyncio
import asyncio, aiohttp
import multiprocessing, threading
from multiprocessing.managers import ValueProxy
import time
from warcio.archiveiterator import ArchiveIterator
import shutil

# setup
log = logging.getLogger("cc-capstone")
mongo_client = pymongo.MongoClient(vars.mongo.mongo_uri)
async_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(vars.mongo.mongo_uri)

db = mongo_client[vars.mongo.db]
async_db = async_mongo_client[vars.mongo.db]

col_warc = db["warc"]
async_col_warc = async_db["warc"]

col_target = db["target"]
async_col_target = async_db["target"]

mongo_queue = vars.mongo.QueueMongo(
    async_col_warc,
    batch_size = vars.settings.batch_size
)

warc_filter = DefaultHandler()

async def consumer_thread(
        id: str,
        queue: multiprocessing.Queue,
        count: ValueProxy,
        lock: threading.Lock,
        kill: threading.Event
):
    # aiohttp sessions like being run inside coroutine tasks. Otherwise would move up with other initializers.
    aiohttp_session = aiohttp.ClientSession()

    while True:
        if kill.is_set():
            log.warning(f"Consumer thread recieved kill signal. ({id})")
            await aiohttp_session.close()
            break

        if queue.empty():
            log.warning(f"(Main) Queue is empty! ({id})")

            time.sleep(vars.settings.wait_tick_speed)
            continue

        task = queue.get()
        task_id = f"filename: {task['filename']} of {task['n_records']} length."

        boundary = b""
        backoff = 0
        fetching = True

        while fetching:
            with open(os.path.join(vars.settings.base_dir, task["filename"]), "wb") as f:
                async with aiohttp_session.get(
                    url=task["url"],
                    headers={"Range": f"bytes={task['range']}"},
                    raise_for_status=True
                ) as response:
                    if response.status == 503:
                        log.error(f"Consumer ({id}): Common Crawl under high load! 503 recieved. Waiting {vars.settings.warc['backoff']} seconds. {task_id}")
                        
                        backoff += 1
                        await asyncio.sleep(vars.settings.warc["backoff"] ** (1 + backoff)) # exponential backoff - stop at max_retries

                        if backoff >= vars.settings.warc["max_retries"]:
                            log.error(f"Consumer thread ({id}) recieved too many 503s! {task_id}")
                            
                            fetching = False
                            break

                    if response.status != 206:
                        log.error(f"Consumer thread ({id}) recieved bad data from queue! {task_id}")
                    
                    elif response.status == 206:
                        boundary = b"--" + response.headers["Content-Type"].split("boundary=")[1].encode("utf-8")

                        async for chunk in response.content.iter_chunked(vars.settings.chunk_size):
                            f.write(chunk)
                        
                        fetching = False # breakout
        
        if not boundary:
            log.error(f"Consumer thread ({id}) recieved bad boundary! {task_id}")

        n_boundaries = 0
        flag = 5

        with open(os.path.join(vars.settings.base_dir, task["filename"]), "rb") as f_in, open(os.path.join(vars.settings.base_dir, task["filename"] + ".tmp"), "wb") as f_out:

            for line in f_in:
                if flag > 0:
                    flag -= 1
                    continue
                
                if line.startswith(boundary):
                    n_boundaries += 1
                    flag = 3

                    f_out.seek(f_out.tell() - len(b"\r\n"))
                    continue
                
                f_out.write(line)
            
            f_out.seek(f_out.tell() - len(b"\r\n"))
            f_out.truncate()
        
        os.replace(os.path.join(vars.settings.base_dir, task["filename"] + ".tmp"), os.path.join(vars.settings.base_dir, task["filename"]))

        with open(os.path.join(vars.settings.base_dir, task["filename"]), "rb") as f:
            targets = []
            idx = 0
            
            for warc in ArchiveIterator(f):
                html = warc.content_stream().read()
                schema = warc_filter.filter(html)
                
                if not schema:
                    idx += 1
                    continue
                
                targets.append({
                    "cc_filename": task["filename"],
                    "warc_mid": task["m_ids"][idx],
                    "warc_id": warc.rec_headers.get_header("WARC-Record-ID"),
                    "target_url": warc.rec_headers.get_header("WARC-Target-URI"),
                    "timestamp": warc.rec_headers.get_header("WARC-Date"),
                    "html": html.decode("utf-8", errors="ignore"),
                })

                idx += 1
            
            if len(targets) > 0:
                col_target.insert_many(targets)

        os.remove(os.path.join(vars.settings.base_dir, task["filename"]))
        col_warc.update_many({ "_id": { "$in": task["m_ids"] } }, { "$set": { "status": "processed" } })

        if n_boundaries and task["n_records"] != n_boundaries:
            log.error(f"Consumer thread ({id}) recieved mismatching boundaries! {task_id}")

        with lock:
            count.value += n_boundaries
        
        log.debug(f"Consumer thread ({id}) completed task of {n_boundaries} warcs. {task_id}")

async def main(
        id: str,
        queue: multiprocessing.Queue,
        c: ValueProxy,
        lock: threading.Lock,
        kill: threading.Event
):
    # filter warcs by status, group warc files by url, sort by offset
    # for each url, process warc files

    log.info(f"{id} worker started!")

    thread_kill = threading.Event()

    threads: list[threading.Thread] = []

    for i in range(vars.settings.warc["worker"]["threads"]):
        tmp_id = f"{id}-THREAD-{i}"
        
        t = threading.Thread(
            target=asyncio.run,
            args=(consumer_thread(
                id,
                queue,
                c,
                lock,
                thread_kill
            ), )
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
                    log.warning("All threads killed!")
                    
                    break
                else:
                    log.warning(f"{id} waiting for threads to die...")
                    time.sleep(vars.settings.wait_tick_speed)
            
            return
        
        await asyncio.sleep(vars.settings.wait_tick_speed)
        log.debug(f"Completed: {c.value}")
    

