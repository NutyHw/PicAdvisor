import os 
import json 
import tweepy 
import time
import random
from dotenv import load_dotenv
from pymongo import MongoClient
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor

load_dotenv( '../env/mongo_auth.env' )

def load_app() -> dict:
    with open( '../env/twitter_app.json' ) as f:
        return json.load( f )

def auth_app( consumer_key : str, consumer_secret : str, access_token : str, access_token_secret : str ) -> tweepy.API:
    auth = tweepy.OAuthHandler( consumer_key, consumer_secret ) 
    auth.set_access_token( access_token, access_token_secret )
    return tweepy.API( auth )

def get_rate_limt( api : tweepy.API ) -> dict:
    rate_limit = api.rate_limit_status( 'search' )
    return rate_limit['resources']['search']['/search/tweets']

def process_search_result( search_result : list ):
    records = list()
    for tweet in search_result:
        if tweet.id in visited_ids or 'media' not in tweet.entities.keys():
            continue

        record = {
            'id' : tweet.id,
            'text' : tweet.text,
            'retweet_count' : tweet.retweet_count,
            'favorite_count' : tweet.favorite_count,
            'hashtags' : [ hashtag['text'] for hashtag in tweet.entities['hashtags'] ],
            'image_urls' : [ media['media_url'] for media in tweet.entities['media']  if media['type'] == 'photo' ]
        } 


        for hashtag in record['hashtags']:
            if hashtag not in visited_hashtags and hashtag not in irr_hashtags and hashtag not in frontier:
                frontier.append( hashtag )

        records.append( record )

    return records

def collect_data( api : tweepy.API, query : str, frontier : list, visited_hashtags : set, visited_ids : set, irr_hashtags : set ) -> tuple:

    rate_limit = get_rate_limt( api )
    remaining = rate_limit['remaining']

    if rate_limit['remaining'] == 0:
        print('sleep')
        time.sleep( rate_limit['reset'] - time.time() )
        rate_limit = get_rate_limt( api )
        remaining = rate_limit['limit']

    records = list()

    max_id = 1e25
    count = 0

    for i in range(remaining):
        search_result = api.search( q = '#' + query, result = 'popular', lang = 'th', count = 100, max_id = max_id )

        if len(search_result) == 0:
            break

        count += len(search_result)

    return records, len(records) / count

def insert_data( records : list ) -> None:
    client = MongoClient(
        host=os.getenv('HOST'),
        username=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD'),
        authSource=os.getenv('AUTHSOURCE'),
        authMechanism=os.getenv('AUTHMECH')
    )

    db = client.get_database( os.getenv('AUTHSOURCE') )
    db.get_collection( 'tweets' ).insert_many( records )

if __name__ == '__main__':
    apps = load_app()

    apis = list()

    for key in apps:
        apis.append(auth_app( apps[key]['API_KEY'], apps[key]['API_SECRET_KEY'], apps[key]['ACCESS_TOKEN'], apps[key]['ACCESS_TOKEN_SECRET'] ))

    random.shuffle( apis ) 
    apis = cycle( apis )

    frontier = [ 'ReviewThailand', 'unseenthailand', 'amazingthailand', 'TourismAuthorityOfThailand', 'thailand' ]
    visited_hashtags = set()
    visited_ids = set()
    irr_hashtags = set()

    records = list()

    while len( frontier ) > 0:
        if len(records) > 1000000:
            insert_data( records )

        hashtag = frontier.pop( 0 )
        api = next(apis)

        data, scores = collect_data( api, hashtag, frontier, visited_hashtags, visited_ids, irr_hashtags )

        visited_hashtags = visited_hashtags.union( [ hashtag ] )
        visited_ids = visited_ids.union( [ record['id'] for record in data ] )
        
        if scores < 0.05:
            irr_hashtags = irr_hashtags.union( hashtag )

        records += data

    with open( 'visited_ids.json', 'w' ) as f:
        json.dump( list(visited_ids), f )

    with open('irr_hashtags.json', 'w') as f:
        json.dump( list( irr_hashtags ), f )
