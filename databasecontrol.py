from pymongo import MongoClient
from cfg import *


# connecting to database and clearing previous data
cluster=MongoClient(port=27017)

db=cluster[database_name]
# if data is already present then drop(helpful during testing)
db[collection_name].drop()
collection=db[collection_name]
# initially adding root url to data
collection.insert_one({"link":ROOT_URL,
            "source_link":"rooturl", 
            "is_crawled":False,
            "last_crawl_dt":None, 
            "Responce_status":None, 
            "content_type":None, 
            "content_length":None, 
            "file_path":None, 
            "created_at":None})

# function to update collection with changing created at(for updating not crawled data)
def update_collection(url,collection, http_status=None,content_length=None,content_type=None,file_name=None):
    collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "Responce_status":http_status,
                                                "content_type":content_type,
                                                "content_length":content_length,
                                                "file_path":file_name,
                                                "created_at":datetime.now()}})

# function to update collection without changing created at(for updating old data)
def update_collection_old(url,collection, http_status=None,content_length=None,content_type=None,file_name=None):
    collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "Responce_status":http_status,
                                                "content_type":content_type,
                                                "content_length":content_length,
                                                "file_path":file_name}})

# function to create new link during crawling
def create_new_link(url,link,collection):
    collection.insert_one({"link":link, "source_link":url, 
                            "is_crawled":False,"last_crawl_dt":None, 
                            "Responce_status":None, "content_type":None, 
                            "content_length":None, "file_path":None, 
                            "created_at":None})
