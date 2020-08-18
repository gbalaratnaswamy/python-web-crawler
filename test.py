import pandas as pd
import sys
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import string
import random
ascii_letters = string.ascii_lowercase

MAX_DATA_LIMIT=500
# one day
CRAWL_AFTER=datetime(2020,8,2)-datetime(2020,8,1)
# connecting to database
cluster=MongoClient(port=27017)
db=cluster['webcrawler']
collection=db['webcrawler']
daybefore=datetime.now()-CRAWL_AFTER
print(daybefore)
root=collection.find({"last_crawl_dt": {"$lt": daybefore}})
for data in root:
    print(data["last_crawl_dt"])


