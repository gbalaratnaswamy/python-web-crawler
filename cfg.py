from datetime import datetime

# defining constants
MAX_DATA_LIMIT=5000
CRAWL_AFTER=datetime(2020,8,2)-datetime(2020,8,1)
DELAY_TIME=5
ROOT_URL="https://flinkhub.com"
DATABASE_NAME="webcrawler"
COLLECTION_NAME="webcrawler"
DATA_EXCEED_DELAY=10
THREAD_COMPLETE_DELAY=1
MULTITHREAD_THRESHOLD=5
NO_OF_THREADS=5