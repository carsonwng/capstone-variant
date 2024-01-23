import os

tick_speed = 2
wait_tick_speed = 10
chunk_size = 8192
batch_size = 1000

base_dir = "../data"

accepted_mimes = [
    "text/html",
    
]

crawl = {
    "backoff": 1,
    
    # "cdx_threshold": 10,
    "cdx_threshold": -1
}

cdx = {
    "backoff": 1,

    # "warc_threshold": 1_000_000
    "warc_threshold": -1
}

warc = {
    "manager": {
        "threads": 10
    },

    "worker": {
        "processes": 2,
        "threads": 2
    },

    "backoff": 1,
    "max_retries": 5,

    "query_limit": 100
}