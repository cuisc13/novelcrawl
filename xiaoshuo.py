#-*-coding:utf-8-*-

import requests
#import re
#from bs4 import BeautifulSoup
from lxml import html
from pymongo import MongoClient
from bson import ObjectId

import logging
log_path = "./log.log"
logger = logging.getLogger(__name__)

fh = logging.FileHandler(log_path)
fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
datefmt = "%a %d %b %Y %H:%M:%S"
formatter = logging.Formatter(fmt, datefmt)
fh.setFormatter(formatter)

logger.addHandler(fh)
logger.setLevel(logging.DEBUG)
logger.info("asg")
from redis import Redis
r = Redis()
#r.set('netnovel_id', 1)

c = MongoClient()
db = c.novel
col = db.netnovel

max_pn = 8700
url = 'http://www.mfxsydw.com/book/%d.html'

host = 'http://www.mfxsydw.com'




def get_chapter_list(tree):
    #chapterlist = re.findall(r'(?<=href=\")\/book\/.+?(?=\")',con)
    boxs =tree.xpath('//div[@class="box"]')
    boxs = boxs[2:]
    juan = []
    for box in boxs:
        juan_name = box.xpath('//h3/span/text()')[0]
        chapter_url_element = box.xpath('//ul/li/a')
        chapter_url_list = [a.attrib['href'] for a in chapter_url_element]
        juan.append(dict(juan_name=juan_name, chapter_url_list = chapter_url_list))

    return juan
    #return chapterlist

def get_chapter_content(c,name):
    url = host + c
    try:
        res = requests.get(url, timeout=2)
    except requests.exceptions.ConnectionError:
        res = requests.get(url)
    tree = html.fromstring(res.content)
    title = tree.xpath('//h1[@itemprop="headline"]/text()')[0]
    logger.info(title)
    body_list = tree.xpath('//div[@itemprop="articleBody"]/p/text()')
    body = "\n".join(body_list)
    cid = str(ObjectId())
    chapter = dict(cid=cid, title=title, body=body)
    return chapter

def main():
    pn = 0
    while pn<max_pn:
        #pn += 1
        pn = r.incr('netnovel_id')
        try:
            res = requests.get(url % pn, timeout=2);
        except requests.exceptions.ConnectionError:
            res = requests.get(url % pn);
        con = res.content
        tree = html.fromstring(con)
        name = tree.xpath('//span[@itemprop="name"]/text()')[0]
        author, ntype, _ = tree.xpath('//p[@class="info"]/span/text()')
        juan_list = get_chapter_list(tree)
        nid = str(ObjectId())
        novel = dict(
            _id = nid,
            name = name,
            author = author,
            type = ntype,
            juan_list = []
        )
        col.insert_one(novel)
        for juan in juan_list:
            chapter_url_list = juan['chapter_url_list']
            juan_id = str(ObjectId())
            juan = dict(jid = juan_id, juan_name = juan['juan_name'], chapterlist=[])
            col.update({'_id':nid}, {"$push":{"juan_list":juan}})
            for i, c in enumerate(chapter_url_list):
                chapter = get_chapter_content(c,name)
                chapter['index'] = i
                col.update({'juan_list.jid':juan_id},{"$push":{"juan_list.$.chapterlist":chapter}})
        break

if __name__ == '__main__':
    pass
    main()
