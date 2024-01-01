from dotenv import load_dotenv
import os

load_dotenv()

mongo_user, mongo_pass = os.getenv("MONGO_USER"), os.getenv("MONGO_PASS")
mongo_host, mongo_port = os.getenv("MONGO_HOST"), os.getenv("MONGO_PORT")

mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?authMechanism=DEFAULT"

db = "cc-capstone"
collections = ["crawl", "cdx", "warc"]