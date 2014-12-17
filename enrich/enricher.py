import sys,os
import ConfigParser
import concurrent.futures
from geopy.geocoders import Nominatim
from pymongo import MongoClient
from bson.objectid import ObjectId
import collections

Config = ConfigParser.ConfigParser()
Config.read("/etc/tunedmongo.conf")
uri = Config.get("mongo", "uri")
mitie_path = Config.get("mitie","path")
ner_model_path = Config.get("mitie","ner_model")
c = MongoClient(uri)
db = c['tuned-db']
sys.path.append(mitie_path)
from mitie import *

countries = ['india','africa','usa']
ner = named_entity_extractor(ner_model_path)
geolocator = Nominatim()

class GeoMemo(object):
    def __init__(self,func):
        self.func = func
        self.db_cache = db.locations
    def __call__(self,*args):
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if self.db_cache.find({'location':args[0]}).count()>0:
            return self.db_cache.find({'location':args[0]})[0]
        else:
            value = self.func(*args)
            if type(args[0]) is dict:
                args[0] = tuple(args[0])
            if value:
                id = self.db_cache.insert({'location':args[0],'osm_data':value})
                print id
            return value

@GeoMemo
def get_geo(loc):
    geo = geolocator.geocode(loc)
    if geo:
        return geo.raw
    else:
        return None
    
def enrich_article(article_id,article_text,country):
    try:
        tokens = tokenize(article_text.encode('ascii', 'ignore'))
        entities = ner.extract_entities(tokens)
        meta = {}
        for e in entities:
            range = e[0]
            tag = e[1]
            entity_list = [tokens[i] for i in range]
            if tag in meta:
                meta[tag].extend(entity_list)
            else:
                meta[tag] = entity_list
                
        if 'LOCATION' in meta:
            geo_data = [get_geo(loc) for loc in meta['LOCATION']]
            meta['geo'] = geo_data

        res = db.posts.update({'_id':article_id},{"$set":meta})
        print db.posts.find_one({"_id":{"oid":article_id}})
            
    except Exception as e:
        print e

def stream_country_articles(country):
    for article in db.articles.find({'country':country}):
        if article:
            yield article['_id'],article['text']
                

if __name__=="__main__":
    for country in countries:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            for article in stream_country_articles(country):
                executor.submit(enrich_article,article[0],article[1],country)
    
    



        
    


