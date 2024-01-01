# starting the logger module
import sys, os
sys.path.append('..')

import startup

import vars.mongo, vars.commoncrawl, vars.settings

import logging
import pymongo
import motor.motor_asyncio
import asyncio
import requests
import gzip
import json
from tqdm import tqdm

# setup
log = logging.getLogger("cc-capstone")
mongo_client = pymongo.MongoClient(vars.mongo.mongo_uri)
async_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(vars.mongo.mongo_uri)

db = mongo_client[vars.mongo.db]
async_db = async_mongo_client[vars.mongo.db]

col_cdx = db["cdx"]
async_col_cdx = async_db["cdx"]

col_warc = db["warc"]
async_col_warc = async_db["warc"]

# if cdx collection is empty, throw error
if col_cdx.count_documents({}) == 0:
    log.error("CDX collection is empty, closing...")
    mongo_client.close()

# main loop
async def main():
    try:
        while True:
            log.debug("Checking for CDX files to process")

            warc_left = col_warc.count_documents({ "status": "not_processed" })

            if vars.settings.cdx["warc_threshold"] != -1 and warc_left >= vars.settings.cdx["warc_threshold"]:
                continue
            

            if col_cdx.count_documents({ "status": "not_processed" }) == 0:
                log.warning("Waiting for CDX files to process...")
                
                await asyncio.sleep(vars.settings.wait_tick_speed)

            cdx = col_cdx.find_one({ "status": "not_processed" })
            
            cdx_url = f"{vars.commoncrawl.base_url}{cdx['id']}"
            cdx_dir = f"{vars.settings.base_dir}/{cdx['id'].split('/')[-1]}"

            log.info(f"Ingesting {cdx['name']}")

            with requests.Session() as session:
                with session.get(cdx_url, stream=True) as r:
                    r.raise_for_status()

                    with open(cdx_dir, "wb") as f:
                        download_progress = tqdm(unit="B", unit_scale=True, total=int(r.headers["Content-Length"]))
                        download_progress.set_description(f"Downloading {cdx['name'].split('/')[-1]}")

                        for chunk in r.iter_content(chunk_size=vars.settings.chunk_size):
                            f.write(chunk)

                            download_progress.update(len(chunk))

                        download_progress.close()

                    with gzip.open(cdx_dir, "rt") as f:
                        process_progress = tqdm(total=int(os.popen(f"wc -l {cdx_dir}").read().split(" ")[0]), unit="lines", unit_scale=True) # os.popen is the ungodly way to check file size (in terms of lines)
                        process_progress.set_description(f"Processing {cdx['name'].split('/')[-1]}")

                        async_tasks = []

                        for line in f:
                            target, timestamp, warc_meta = line.split(" ", 2)

                            warc_meta: dict = json.loads(warc_meta)

                            if warc_meta["status"] != "200":
                                continue

                            if warc_meta["mime-detected"] not in vars.settings.accepted_mimes:
                                continue
                            
                            async_tasks.append(async_col_warc.insert_one({
                                "name": target,
                                "crawl_id": cdx["crawl_id"],
                                "crawl_mid": cdx["crawl_mid"],
                                "cdx_id": cdx["id"],
                                "cdx_mid": cdx["_id"],
                                "status": "not_processed",
                                "length": warc_meta["length"],
                                "offset": warc_meta["offset"],
                                "filename": warc_meta["filename"],
                                "warc_timestamp": timestamp
                            }))

                            process_progress.update(1)
                        
                        process_progress.close()

                        log.debug(f"Waiting for {len(async_tasks)} warcs to process")
                        # wait for all warcs to be processed
                        await asyncio.gather(*async_tasks)

                        col_cdx.update_one({ "_id": cdx["_id"] }, { "$set": { "status": "processed" } })
                        log.info(f"{cdx['name']} processed")
                        
    
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt, closing...")
        mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())