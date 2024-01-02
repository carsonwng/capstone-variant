from dotenv import load_dotenv
import os

# from pymongo import InsertOne
# from pymongo.collection import Collection

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

load_dotenv()

mongo_user, mongo_pass = os.getenv("MONGO_USER"), os.getenv("MONGO_PASS")
mongo_host, mongo_port = os.getenv("MONGO_HOST"), os.getenv("MONGO_PORT")

mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?authMechanism=DEFAULT"

db = "cc-capstone"
collections = ["crawl", "cdx", "warc"]

class QueueMongo:
    def __init__(self, col, batch_size = 1000):
        self.col: AsyncIOMotorCollection = col
        self.client: AsyncIOMotorClient = AsyncIOMotorClient()
        self.session = self.client.start_session()
        
        self.ops: list = []
        self.ops_len: int = 0

        self.batch_size: int = batch_size

        print(f"QueueMongo initialized with batch size of {self.batch_size}")

    def _check_batch(self):
        if self.ops_len >= self.batch_size:
            old_len = self.ops_len
            
            
            self.col.insert_many(self.ops, session=self.session)
            self.ops = []
            self.ops_len = 0

            return old_len

        return 0

    def insert_one(self, doc):
        try:
            self.ops.append(doc)
            self.ops_len += 1
            
            result = self._check_batch()
            return result
        
        except Exception as e:
            raise e
    
    async def close(self):
        if len(self.ops) > 0:
            await asyncio.gather(*self.col.insert_many(self.ops, session=self.session))
            self.session.close()

