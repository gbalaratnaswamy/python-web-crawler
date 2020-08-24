#

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import string
import random
import time
import threading
from cfg import *
from crawl_func import crawl_data,start_threads,collection

# start crawler 
while True:
    # crawl for not crawled links
    not_crawled_data=list(collection.find({"is_crawled":False}))

    # if data is very less then no point of multithreading(i.e during first run only 1 data will be present)
    len_of_data=len(not_crawled_data)
    if len_of_data<MULTITHREAD_THRESHOLD:
        crawl_data(not_crawled_data)

    # multithread work
    else :
        mythread= start_threads(data_list=not_crawled_data)
        mythread.start()
        # wait until all theads are completed
        while mythread.is_not_complete():
            time.sleep(THREAD_COMPLETE_DELAY)  # wait for one second and check again if threads are complete

    if collection.count_documents({})>MAX_DATA_LIMIT:
        print("data limit exceed from main thread")
        while collection.count_documents({})>MAX_DATA_LIMIT:
            time.sleep(DATA_EXCEED_DELAY)  # wait for 10 sec in expecting data was cleaned by user

    # crawl for data that donot crawled for last one day
    daybefore=datetime.now()-CRAWL_AFTER
    old_data=list(collection.find({"last_crawl_dt":{"$lt":daybefore}}))
    if len(old_data)<MULTITHREAD_THRESHOLD:
        crawl_data(old_data,new=False)
    else :
        mythread= start_threads(data_list=old_data,new=False)
        mythread.start()
        while mythread.is_not_complete():
            time.sleep(THREAD_COMPLETE_DELAY)  # wait for one second and check again if threads are complete
    if collection.count_documents({})>MAX_DATA_LIMIT:
        print("data limit exceed from main thread")
        while collection.count_documents({})>MAX_DATA_LIMIT:
            time.sleep(DATA_EXCEED_DELAY)  # wait for 10 sec in expecting data was cleaned by user