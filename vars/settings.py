import os

tick_speed = 2
wait_tick_speed = 10
chunk_size = 8192

base_dir = "../data"

accepted_mimes = [
    "text/html",
    
]

crawl = {
    "backoff": 1,
    "max_retries": 5,
    "timeout": 10,
    
    # "cdx_threshold": 10,
    "cdx_threshold": -1
}

cdx = {
    "backoff": 1,
    "max_retries": 5,
    "timeout": 10,

    # "warc_threshold": 1_000_000
    "warc_threshold": -1
}