# starting the logger module
import sys, os
sys.path.append('..')

import startup
import vars.mongo, vars.commoncrawl, vars.settings
from manager import main as manager
from worker import main as worker

import logging
# import pymongo
# import motor.motor_asyncio
import asyncio
import multiprocessing

# # setup
log = logging.getLogger("cc-capstone")
# mongo_client = pymongo.MongoClient(vars.mongo.mongo_uri)
# async_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(vars.mongo.mongo_uri)

# db = mongo_client[vars.mongo.db]
# async_db = async_mongo_client[vars.mongo.db]

# col_warc = db["warc"]
# async_col_warc = async_db["warc"]

# mongo_queue = vars.mongo.QueueMongo(
#     async_col_warc,
#     batch_size = vars.settings.batch_size
# )

def start_process(fn: callable, args: tuple):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(fn(*args))
    finally:
        loop.close()

async def main():
    try:
        m = multiprocessing.Manager()
        q = m.Queue()

        c = m.Value('i', 0)
        lock = m.Lock()

        kill = multiprocessing.Event()

        child_processes: list[multiprocessing.Process] = []

        # child_processes.append(multiprocessing.Process(manager, args=(q, kill)), daemon=False)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        id = "MANAGER-1"

        p = multiprocessing.Process(target=start_process, args=(
            manager,
            (id, q, kill)
        ))        
        # p = multiprocessing.Process(target=loop.run_until_complete, args=manager(q, kill))

        p.start()
        child_processes.append(p)

        for pdx in range(vars.settings.warc["worker"]["processes"]):

            id = f"WORKER-{pdx}"

            p = multiprocessing.Process(target=start_process, args=(
                worker,
                (id, q, c, lock, kill)
            ))

            p.start()
            child_processes.append(p)

        while True: # trap children for KeyboardInterrupt
            await asyncio.sleep(vars.settings.tick_speed)
            
    except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
        kill.set()
        
        log.warning("KeyboardInterrupt detected, waiting for children to die...")

        while True:
            if all(not p.is_alive() for p in child_processes):
                log.warning("All children died!")
                break

if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(main())
