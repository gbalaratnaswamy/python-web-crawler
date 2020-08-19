import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import string
import random
import time
import threading

# multithreading work
class thread_crawl(threading.Thread):
    def __init__(self, thread_id,data_list,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=True):
        threading.Thread.__init__(self)
        self.thread_id=thread_id
        self.data_list=data_list
        self.DELAY_TIME=DELAY_TIME
        self.CRAWL_AFTER=CRAWL_AFTER
        self.MAX_DATA_LIMIT=MAX_DATA_LIMIT
        self.collection=collection
        self.new=new
    def run(self):
        crawl_data(self.data_list,self.DELAY_TIME,self.CRAWL_AFTER,self.MAX_DATA_LIMIT,self.collection,self.new)

        
# function to save files
def write_to_file(file_name,html_text):
    try:
        with open("files/"+file_name,"wb") as f:
            f.write(html_text)
    except UnicodeEncodeError:
        file_name=None
    return file_name

# function to update collection with changing created at
def update_collection(url,collection, http_status=None,content_length=None,content_type=None,file_name=None):
    collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "Responce_status":http_status,
                                                "content_type":content_type,
                                                "content_length":content_length,
                                                "file_path":file_name,
                                                "created_at":datetime.now()}})

# function to update collection without changing created at
def update_collection_old(url,collection, http_status=None,content_length=None,content_type=None,file_name=None):
    collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "Responce_status":http_status,
                                                "content_type":content_type,
                                                "content_length":content_length,
                                                "file_path":file_name}})

# function to create new link
def create_new_link(url,link,collection):
    collection.insert_one({"link":link, "source_link":url, 
                            "is_crawled":False,"last_crawl_dt":None, 
                            "Responce_status":None, "content_type":None, 
                            "content_length":None, "file_path":None, 
                            "created_at":None})

# function to generate random sting
def generate_random_string():
    ascii_letters = string.ascii_lowercase
    file_name = ''.join(random.choice(ascii_letters) for i in range(10))
    return file_name

# handel content type of applications
def handel_applications(content_type):
    file_name=generate_random_string()
    if content_type[11:]=="/pdf":
        file_name+=".pdf"
    elif content_type[11:]=="/json":
        file_name+=".json"
    elif content_type[11:]=="/xml":
        file_name+=".xml"
    elif content_type[11:]=="/javascript":
        file_name+=".js"
    elif content_type[11:]=="/zip":
        file_name+=".zip"
    elif content_type[11:]=="/x-7z-compressed":
        file_name+=".7z"
    elif content_type[11:]=="/vnd.mozilla.xul+xml":
        file_name+=".xul"
    elif content_type[11:]=="/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        file_name+=".xlsx"
    elif content_type[11:]=="/vnd.ms-excel":
        file_name+=".xls"
    elif content_type[11:]=="/xhtml+xml":
        file_name+=".xhtml"
    elif content_type[11:]=="/x-tar":
        file_name+=".tar"
    elif content_type[11:]=="/x-sh":
        file_name+=".sh"
    elif content_type[11:]=="/rtf":
        file_name+=".rtf"
    elif content_type[11:]=="/vnd.rar":
        file_name+=".rar"
    elif content_type[11:]=="/vnd.openxmlformats-officedocument.presentationml.presentation":
        file_name+=".pptx"
    elif content_type[11:]=="/vnd.ms-powerpoint":
        file_name+=".ppt"
    elif content_type[11:]=="/x-httpd-php":
        file_name+=".php"
    # for other file types leave
    else :
        file_name=None
    return file_name

# handel content type of audio
def handel_audio(content_type):
    file_name=generate_random_string()
    if content_type[5:]=="/acc":
        file_name+=".acc"
    elif content_type[5:]=="/ogg":
        file_name+=".ogg"
    elif content_type[5:]=="/opus":
        file_name+=".ogg"
    elif content_type[5:]=="/wav":
        file_name+=".wav"
    elif content_type[5:]=="/webm":
        file_name+=".webm"
    elif content_type[5:]=="/3gpp":
        file_name+=".3gp"
    elif content_type[5:]=="/3gpp2":
        file_name+=".3g2"
    else :
        file_name=None
    return file_name

# handel content type of text
def handel_text(content_type):
    file_name=generate_random_string()
    if content_type[4:]=="/plain":
        temp=url.split(".")
        file_name+="."+temp[-1]
    elif content_type[:4]=="/xml":
        file_name+=".xml"
    elif content_type[:4]=="/javascript":
        file_name+=".xml"
    elif content_type[:4]=="/csv":
        file_name+=".xml"
    elif content_type[:4]=="/css":
        file_name+=".xml"
    elif content_type[:4]=="/css":
        file_name+=".xml"
    else :
        file_name=None
    return file_name

# hadel types other than html
def other_content_types(url,collection,http_status,content_length,content_type,html_text,new=True):
     # if it is application
    file_name=None
    if content_type[:11]=="application":
        file_name=handel_applications(content_type)
    # if content type is audio
    elif content_type[:5]=="audio":
        file_name=handel_audio(content_type)
    
    # if content type is text
    elif content_type[:4]=="text":
        file_name=handel_text(content_type)
    
    # if content type is image
    elif content_type[:5]=="image":
        file_name=handel_image(content_type)
    
    # if content type is video
    elif content_type[:5]=="video":
        file_name=handel_video(content_type)
    
    if file_name!=None:
        file_name=write_to_file(file_name,html_text)
        if new:
            update_collection(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)
        else:
            update_collection_old(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)

# handel html
def handel_html(url,html_text,http_status,collection,content_type,content_length,new=True):
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
            # links like example.com and example.com/ are same
            if link[-1]=="/":
                link=link[:-1]
        # if link is empty string
        except (IndexError ,TypeError):
            # print(link,"is not a valid link")
            continue
        # if link not present in data base then add it
        if collection.find_one({"link":link})==None:
            create_new_link(url,link,collection)
    file_name=generate_random_string()
    file_name+=".html"
    file_name=write_to_file(file_name,html_text)
    if new:
        update_collection_old(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)
    else :
        update_collection_old(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)
    # print(url,"is sucessfully crawled and data updated")


def crawl_data(data_list,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=True):
    for data in data_list:
        url=data['link']
        try:
            resp=requests.get(url)
        # network error then marked as crawled and crawl after 24 hr
        except (requests.ConnectionError ,requests.ConnectTimeout ,requests.HTTPError):
            if new:
                update_collection(url,collection)
            else :
                update_collection_old(url,collection)
            continue
        # obtain data
        html_text=resp.text
        http_status=resp.status_code
        headers=resp.headers

        # if http status is grater than 400 implies client side error or server side errror
        if http_status>=400:
            if new:
                update_collection(url,collection,http_status=http_status)
            else :
                update_collection_old(url,collection,http_status=http_status)
            continue
        try:
            content_length=headers['content-length']
        # if content length not present
        except KeyError:
            content_length=len(html_text)
        content_type=headers['content-type'].split(";")
        content_type=content_type[0]
        # setting html to content for easy wirting without errors
        html_text=resp.content
        # if responce is html then crawl as only html contains links
        if content_type=="text/html":
            handel_html(url,html_text,http_status,collection,content_type,content_length,new)
        # if responce is not html then 
        else :
            other_content_types(url,collection,http_status,content_length,content_type,html_text)
        time.sleep(DELAY_TIME)

        # if data limit exceed
        if collection.count_documents({})>MAX_DATA_LIMIT:
            print("data limit exceed")
            return


# constants
MAX_DATA_LIMIT=5000
CRAWL_AFTER=datetime(2020,8,2)-datetime(2020,8,1)
DELAY_TIME=5

# connecting to database and clearing previous data
cluster=MongoClient(port=27017)
db=cluster['webcrawler']
db.webcrawler.drop()
collection=db['webcrawler']

# initially adding root url to data
initial_data={"link":"http://www.pdf995.com/samples/pdf.pdf", 
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
    # crawl for not crawled links
    not_crawled_data=list(collection.find({"is_crawled":False}))
    # if data is very less then no point of multithreading
    len_of_data=len(not_crawled_data)
    if len_of_data<5:
        crawl_data(not_crawled_data,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
    # multithread work
    else :
        thread_dividing=int(len_of_data/5)
        thread1=thread_crawl(1,not_crawled_data[:thread_dividing],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
        thread2=thread_crawl(2,not_crawled_data[thread_dividing:thread_dividing*2],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
        thread3=thread_crawl(3,not_crawled_data[thread_dividing*2:thread_dividing*3],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
        thread4=thread_crawl(4,not_crawled_data[thread_dividing*3:thread_dividing*4],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
        thread5=thread_crawl(5,not_crawled_data[thread_dividing*4:],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()
        while thread1.isAlive() or thread2.isAlive() or thread3.isAlive() or thread4.isAlive() or thread5.isAlive():
            time.sleep(1)  # wait for one second and check again if threads are complete
    if collection.count_documents({})>MAX_DATA_LIMIT:
        print("data limit exceed from main thread")
        while collection.count_documents({})>MAX_DATA_LIMIT:
            time.sleep(10)  # wait for 10 sec in expecting data was cleaned by user
    # crawl for data that donot crawled for last one day
    daybefore=datetime.now()-CRAWL_AFTER
    old_data=list(collection.find({"is_crawled":{"$lt":daybefore}}))
    if len(old_data)<5:
        crawl_data(old_data,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=False)
    else :
        thread_dividing=int(len_of_data/5)
        thread1=thread_crawl(1,old_data[:thread_dividing],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,False)
        thread2=thread_crawl(2,old_data[thread_dividing:thread_dividing*2],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,False)
        thread3=thread_crawl(3,old_data[thread_dividing*2:thread_dividing*3],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,False)
        thread4=thread_crawl(4,old_data[thread_dividing*3:thread_dividing*4],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,False)
        thread5=thread_crawl(5,old_data[thread_dividing*4:],DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,False)
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()
        while thread1.isAlive() or thread2.isAlive() or thread3.isAlive() or thread4.isAlive() or thread5.isAlive():
            time.sleep(1)  # wait for one second to check again if threads work is complete
    if collection.count_documents({})>MAX_DATA_LIMIT:
        print("data limit exceed from main thread")
        while collection.count_documents({})>MAX_DATA_LIMIT:
            time.sleep(10)  # wait for 10 sec in expecting data was cleaned by user