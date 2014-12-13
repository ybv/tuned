import feedparser
from boilerpipe.extract import Extractor
from bs4 import BeautifulSoup
import urllib2
from pymongo import MongoClient
import datetime
import concurrent.futures
import requests
import json
import ConfigParser

Config = ConfigParser.ConfigParser()
Config.read("/etc/tunedmongo.conf")
uri = Config.get("mongo", "uri")
c = MongoClient(uri)
db = c['tuned-db']

def get_links_from_rss_feed(url):
    '''
    returns all links from a feed url
    '''
    feed = feedparser.parse(url)
    return [entry['link'] for entry in feed['entries']]

def ext(link,country,source):
    '''
    extract and save.. replace bs4 soon
    '''
    content = requests.get(link)
    soup = BeautifulSoup(content.text)
    try:
        extractor = Extractor(extractor='CanolaExtractor', html=content.text)
        article = {}
        article['country']= country
        article['source'] = source
        article['title'] = soup.title.string
        article['text'] = extractor.getText()
        article['link'] = link
        article['scraped_at'] = datetime.datetime.utcnow()
        db.articles.insert(article)
    except Exception as e:
        print e

def extract_and_save(country,source,URL):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for list_links in fetch_links_from_rss(URL):
            for link in list_links:
                executor.submit(ext,link,country,source)

def fetch_links_from_rss(RSSLINK):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(RSSLINK)) as executor:
        links_in_urls = {executor.submit(get_links_from_rss_feed,RSSLINK):RSSLINK}
        for future_link in concurrent.futures.as_completed(links_in_urls):
            url = links_in_urls[future_link]
            try:
                links = future_link.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
                yield  None
            else:
                yield links

def stream_feeds(sites_file):
    '''
    loads the whole file to memory
    got to fix that
    '''
    with open(sites_file) as data_file:
        json_data = json.load(data_file)
        for country in json_data:
            for source in json_data[country]:
                if source:
                    if 'feeds' in json_data[country][source]:
                        for feed in json_data[country][source]['feeds']:
                            if country in ["africa","india"]:
                                yield country,source,feed
                                
if __name__=="__main__":
    r = requests.Session()
    for i in stream_feeds('sites.json'):
        extract_and_save(i[0],i[1],i[2])
            
            
        
        

