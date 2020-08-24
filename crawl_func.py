# functions to crawl links
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import string
import random
import time
import threading
from cfg import *
from databasecontrol import *

# creating thread class to use with threading library
class thread_crawl(threading.Thread):
    def __init__(self, thread_id,data_list,new=True):
        threading.Thread.__init__(self)
        self.data_list=data_list
        self.new=new
    def run(self):
        crawl_data(self.data_list,self.new)

# creating and handling threads
class start_threads:
    def __init__(self,data_list,new=True):
        self.data_list=data_list
        self.new=new
        self.threads={}
    def start(self):
        len_of_data=len(self.data_list)
        thread_dividing=int(len_of_data/NO_OF_THREADS)
        for i in range(NO_OF_THREADS):
            self.threads[i]=thread_crawl(1,self.data_list[thread_dividing*i:thread_dividing*(i+1)],self.new)
        for i in range(NO_OF_THREADS):
            self.threads[i].start()
    def is_not_complete(self):
        for i in range(NO_OF_THREADS):
            if self.threads[i].isAlive():
                return True
            return False

# function to save files
def write_to_file(file_name,html_text):
    try:
        with open("files/"+file_name,"wb") as f:
            f.write(html_text)
    except UnicodeEncodeError:
        file_name=None
    return file_name

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
def handel_text(content_type,url):
    file_name=generate_random_string()
    if content_type[4:]=="/plain":
        temp=url.split(".")
        file_name+="."+temp[-1]
    elif content_type[4:]=="/xml":
        file_name+=".xml"
    elif content_type[4:]=="/javascript":
        file_name+=".xml"
    elif content_type[4:]=="/csv":
        file_name+=".xml"
    elif content_type[4:]=="/css":
        file_name+=".xml"
    elif content_type[4:]=="/css":
        file_name+=".xml"
    else :
        file_name=None
    return file_name

# handel content of type image
def handel_image(content_type):
    file_name=generate_random_string()
    if content_type[5:]=="/vnd.microsoft.icon":
        file_name+=".ico"
    elif content_type[5:]=="/jpeg":
        file_name+=".jpg"
    elif content_type[5:]=="/bmp":
        file_name+=".bmp"
    elif content_type[5:]=="/gif":
        file_name+=".gif"
    elif content_type[5:]=="/png":
        file_name+=".png"
    elif content_type[5:]=="/svg+xml":
        file_name+=".svg"
    elif content_type[5:]=="/tiff":
        file_name+=".tiff"
    elif content_type[5:]=="/webp":
        file_name+=".webp"
    else :
        file_name=None
    return file_name
    
# handel videos
def handel_video(content_type):
    file_name=generate_random_string()
    if content_type[5:]=="/x-msvideo":
        file_name+=".avi"
    elif content_type[5:]=="/mpeg":
        file_name+=".mpeg"
    elif content_type[5:]=="/ogg":
        file_name+=".ogv"
    elif content_type[5:]=="/mp2t":
        file_name+=".ts"
    elif content_type[5:]=="/webm":
        file_name+=".webm"
    elif content_type[5:]=="/3gpp":
        file_name+=".3gp"
    elif content_type[5:]=="/3gpp2":
        file_name+=".3g2"
    else :
        file_name=None
    return file_name
    

# hadel types other than html
def other_content_types(url,http_status,content_length,content_type,html_text,new=True):
    file_name=None

    # if it is application
    if content_type[:11]=="application":
        file_name=handel_applications(content_type)
        
    # if content type is audio
    elif content_type[:5]=="audio":
        file_name=handel_audio(content_type)
    
    # if content type is text
    elif content_type[:4]=="text":
        file_name=handel_text(content_type,url)
    
    # if content type is image
    elif content_type[:5]=="image":
        file_name=handel_image(content_type)
    
    # if content type is video
    elif content_type[:5]=="video":
        file_name=handel_video(content_type)
    
    if file_name!=None:
        file_name=write_to_file(file_name,html_text)
        # for updating not crawled data new is true and for crawling old data new is false
        if new:
            update_collection(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)
        else:
            update_collection_old(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)

# handel html
def handel_html(url,html_text,http_status,content_type,content_length,new=True):
    soup=BeautifulSoup(html_text,'html.parser')
    a_tags=soup.find_all("a")
    for tag in a_tags:
        link=tag.get("href")
        # clean link
        try :
            # if link is not http(or https) or starts with / it is not valid
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
    
    # for updating not crawled data new is true and for crawling old data new is false
    if new:
        update_collection(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)
    else :
        update_collection_old(url=url,http_status=http_status,content_length=content_length,content_type=content_type,file_name=file_name,collection=collection)
    # print(url,"is sucessfully crawled and data updated")

# crawl data
def crawl_data(data_list,new=True):
    for data in data_list:
        url=data['link']
        try:
            resp=requests.get(url)
        # network error then marked as crawled and crawl after 24 hr
        except (requests.ConnectionError ,requests.ConnectTimeout ,requests.HTTPError):
            # if new then update with changing created at from none to current date
            if new:
                update_collection(url,collection)
            # else do not update created at
            else :
                update_collection_old(url,collection)
            continue
        # obtain data
        html_text=resp.content
        http_status=resp.status_code
        headers=resp.headers

        # if http status is grater than 400 implies client side error or server side error
        if http_status>=400:
            # for updating not crawled data new is true and for crawling old data new is false
            if new:
                update_collection(url,collection,http_status=http_status)
            else :
                update_collection_old(url,collection,http_status=http_status)
            continue
        try:
            content_length=int(headers['content-length'])
        # if content length not present
        except KeyError:
            content_length=len(html_text)
        content_type=headers['content-type'].split(";")
        content_type=content_type[0]
        # if responce is html then crawl as only html contains links
        if content_type=="text/html":
            handel_html(url,html_text,http_status,content_type,content_length,new)
        # if responce is not html then 
        else :
            other_content_types(url,http_status,content_length,content_type,html_text)
        time.sleep(DELAY_TIME)

        # if data limit exceed
        if collection.count_documents({})>MAX_DATA_LIMIT:
            print("data limit exceed")
            return