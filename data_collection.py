import feedparser
from datetime import datetime
from dateutil import tz
import dateutil 
from nltk import tokenize #need nltk punkt package (nltk.download('punkt'))
import csv
import shelve
import requests
import json

from pymongo import MongoClient


# Connection string
connection_string = "mongodb://localhost:27017/"

# Create a MongoClient
client = MongoClient(connection_string)

# Access the database
db = client["your_database_name"]

# Access a collection
collection = db["your_collection_name"]


#Identify feeds that you'll pull data from
rt = feedparser.parse("https://www.rt.com/rss/usa/")
sputnik = feedparser.parse("https://sputniknews.com/export/rss2/archive/index.xml")
strategic_culture = feedparser.parse("https://www.strategic-culture.org/feed/")
global_research = feedparser.parse("https://www.globalresearch.ca/feed")
katehon = feedparser.parse("https://katehon.com/en/rss.xml")
geopolitica_ru = feedparser.parse("https://www.geopolitica.ru/en/rss.xml")
ruptly = feedparser.parse("https://alert.ruptly.tv/feed/rss")
TwentyFirstCWire = feedparser.parse("https://21stcenturywire.com/feed/")
nypost = feedparser.parse("https://nypost.com/feed/")
foxnews = feedparser.parse("http://feeds.foxnews.com/foxnews/latest")
TASS = feedparser.parse("http://tass.com/rss/v2.xml")
breitbart = feedparser.parse("http://feeds.feedburner.com/breitbart?format=xml")
oann = feedparser.parse("https://www.oann.com/feed/")
blacklisted = feedparser.parse("http://feeds.feedburner.com/blacklistednews/hKxa")
usatoday = feedparser.parse("http://rssfeeds.usatoday.com/usatoday-newstopstories&x=1")
startribune = feedparser.parse("https://www.startribune.com/rss/?sf=1&s=/")
chicagotribune = feedparser.parse("https://www.chicagotribune.com/arcio/rss/category/nation-world/?query=display_date:%5Bnow-2d+TO+now%5D+AND+revision.published:true&sort=display_date:desc#nt=instory-link")
latimes = feedparser.parse("https://www.latimes.com/world-nation/rss2.0.xml#nt=1col-7030col1")
nytimes = feedparser.parse("https://rss.nytimes.com/services/xml/rss/nyt/US.xml")
infowars = feedparser.parse("https://www.infowars.com/rss.xml")
huffpo = feedparser.parse("https://www.huffpost.com/section/front-page/feed")
washingtonpost = feedparser.parse("http://feeds.washingtonpost.com/rss/national")
newsfront = feedparser.parse("https://en.news-front.info/feed/")
newsmax_politics = feedparser.parse("https://www.newsmax.com/rss/Politics/1/")
newsmax_us = feedparser.parse("https://www.newsmax.com/rss/US/18/")
newsmax_health = feedparser.parse("https://www.newsmax.com/rss/Health-News/177/")
newsmax_newsfront = feedparser.parse("https://www.newsmax.com/rss/Newsfront/16/")


def rename_id(feed, entries):
    result = []
    for entry in entries:
        if 'id' in entry:
            entry['_id'] = {'feed':feed['href'], 'id':entry.pop('id')}
            result.append(entry)
    return result

feeds= (rt, sputnik, strategic_culture, katehon, global_research, geopolitica_ru, ruptly, TwentyFirstCWire, nypost, foxnews, TASS, breitbart, oann, usatoday, startribune, chicagotribune, latimes, nytimes, infowars, huffpo, blacklisted, washingtonpost, newsfront, newsmax_politics, newsmax_health, newsmax_newsfront, newsmax_us)

entries = list(entry for feed in feeds for entry in rename_id(feed, feed['entries']))

def dedupe_entries(entries):
    seen_ids = set()
    unique_entries = []
    for entry in entries:
        id = tuple(entry['_id'].values())
        if id not in seen_ids:
            seen_ids.update((id, ))
            unique_entries.append(entry)
    print(seen_ids)
    return unique_entries


entries = dedupe_entries(entries)

new_ids = list(entry['_id'] for entry in entries)

query = {}
query["_id"] = {
    u"$in":
        new_ids
}

projection = {}
projection["_id"] = u"$_id"

existing_ids = list(doc['_id'] for doc in collection.find(query, projection = projection))

insert_many_result = collection.insert_many(entry for entry in entries if entry['_id'] not in existing_ids)
insert_many_result.inserted_ids


len(entries)