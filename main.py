import pandas as pd
import sys
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import string
import random
import time
ascii_letters = string.ascii_lowercase

MAX_DATA_LIMIT=500
CRAWL_AFTER=datetime(2020,8,2)-datetime(2020,8,1)
DELAY_TIME=5

# connecting to database and clearing previous data
cluster=MongoClient(port=27017)
db=cluster['webcrawler']
db.webcrawler.drop()
collection=db['webcrawler']

# initially adding root url to data
initial_data={"link":"https://flinkhub.com", 
            "source_link":"rooturl", 
            "is_crawled":False,
            "last_crawl_dt":None, 
            "Responce_status":None, 
            "content_type":None, 
            "content_length":None, 
            "file_path":None, 
            "created_at":None}
collection.insert_one(initial_data)


# start crawler 
while True:
    # if database limit exceeds
    if collection.count_documents({})>MAX_DATA_LIMIT:
        print("data limit exceed")
        while collection.count_documents({})>MAX_DATA_LIMIT:
            pass

    # crawl for not crawled links
    not_crawled_data=collection.find({"is_crawled":False})
    for data in not_crawled_data:
        url=data['link']
        try:
            resp=requests.get(url)
        # network error then marked as crawled and crawl after 24 hr
        except (requests.ConnectionError ,requests.ConnectTimeout ,requests.HTTPError):
            collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "created_at":datetime.now()}})
            continue

        # obtain data
        html_text=resp.text
        html_status=resp.status_code
        headers=resp.headers

        try:
            content_length=headers['content-length']
        # if content length not present
        except KeyError:
            content_length=len(html_text)
        content_type=headers['content-type']

        # if responce is html then crawl as only html contains links
        if content_type[:9]=="text/html":
            soup=BeautifulSoup(html_text,'html.parser')
            a_tags=soup.find_all("a")
            for tag in a_tags:
                link=tag.get("href")

                # clean link
                try :
                    # if link is not http or starts with / it is not valid
                    if link[:4]!="http" and link[0]!="/":
                        # print(link,"is not a valid link")
                        continue
                    if link[0]=="/":
                        link=url+link
                    if link[-1]=="/":
                        link=link[:-1]
                    if collection.find_one({"link":link})==None:
                        initial_data={"link":link, "source_link":url, 
                                    "is_crawled":False,"last_crawl_dt":None, 
                                    "Responce_status":None, "content_type":None, 
                                    "content_length":None, "file_path":None, 
                                    "created_at":None}
                        collection.insert_one(initial_data)
                # if link is empty string
                except (IndexError ,TypeError):
                    # print(link,"is not a valid link")
                    continue

            file_name = ''.join(random.choice(ascii_letters) for i in range(10))
            file_name+=".html"
            try:
                with open("files/"+file_name,'w') as f:
                    f.write(html_text)
            except UnicodeEncodeError:
                file_name=None
            collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                        "last_crawl_dt":datetime.now(),
                                                        "Responce_status":html_status,
                                                        "content_type":content_type,
                                                        "content_length":content_length,
                                                        "file_path":file_name,
                                                        "created_at":datetime.now()}})
            print(url,"is sucessfully crawled and data updated")
        # if responce is not html then leave
        else :
            collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                        "last_crawl_dt":datetime.now(),
                                                        "Responce_status":html_status,
                                                        "content_type":content_type,
                                                        "content_length":content_length,
                                                        "created_at":datetime.now()}})
            print(url,"is not html")
        time.sleep(DELAY_TIME)
        
    # crawl for data that donot crawled for last one day
    # func
