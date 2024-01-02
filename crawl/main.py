# starting the logger module
import sys, os
sys.path.append('..')

import startup

import vars.mongo, vars.commoncrawl, vars.settings

# external imports
import logging
import pymongo
import requests
import gzip

# setup
log = logging.getLogger("cc-capstone")
mongo_client = pymongo.MongoClient(vars.mongo.mongo_uri)

db = mongo_client[vars.mongo.db]
col_crawl = db["crawl"]
col_cdx = db["cdx"]

# main loop
def main():
    try:
        while True:
            cdx_left = col_cdx.count_documents({ "status": "not_processed" })

            if vars.settings.crawl["cdx_threshold"] != -1 and cdx_left >= vars.settings.crawl["cdx_threshold"]:
                continue

            # get target
            crawl = col_crawl.find_one({ "status": "not_processed" })

            if crawl is None:
                log.info("No more crawls to process")
                break

            cdx_index_url = f"{vars.commoncrawl.base_url}crawl-data/{crawl['id']}/cc-index.paths.gz"
            cdx_index_dir = f"{vars.settings.base_dir}/tmp.paths.gz"
            
            log.info(f"Ingesting {crawl['name']}")

            with requests.get(cdx_index_url, stream=True) as r, open(cdx_index_dir, "wb") as f:
                f.write(r.content)
            
            with gzip.open(cdx_index_dir, "r") as f:
                paths = [line.strip().decode("utf-8") for line in f.readlines()]
                paths = filter(lambda path: path.endswith(".gz"), paths)

                for path in paths:
                    col_cdx.insert_one({
                        "name": path,
                        "crawl_id": crawl["id"],
                        "crawl_mid": crawl["_id"],
                        "id": path,
                        "status": "not_processed"
                    })
                
            # remove tmp.paths.gz
            os.remove(cdx_index_dir)

            # update crawl status
            col_crawl.update_one({ "_id": crawl["_id"] }, { "$set": { "status": "processed" } })
            log.info(f"{crawl['name']} processed")
        
    except KeyboardInterrupt:
        log.warning("KeyboardInterrupt detected, closing...")
        mongo_client.close()
    
    except Exception as e:
        log.error(e)
        mongo_client.close()

if __name__ == "__main__":
    # if crawl collection is empty, populate it
    if col_crawl.count_documents({}) == 0:
        with requests.get(vars.commoncrawl.crawl_index) as r:
            crawl_index = r.json()

            for crawl in crawl_index:
                if "ARC" in crawl["name"]:
                    log.warning(f"Skipping {crawl['name']}")
                    continue

                col_crawl.insert_one({
                    "name": crawl["name"],
                    "id": crawl["id"],
                    "status": "not_processed"
                })
        
        log.info("Crawl collection populated")

    main()