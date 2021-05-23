import os 
import json 
import re
import twitter

from dotenv import load_dotenv
from pymongo import MongoClient
from itertools import cycle
import time

load_dotenv( '../env/mongo_auth.env' )

frontier = []
visited_hashtags = set()
visited_ids = set()
rev_hashtags = set()
records = list()

def build_query( hashtag : str ):
    '''
    building start query from given hashtags

    parameter:
    hashtag : hashtag str to build query
    '''
    return f'q=%23{ hashtag }&lang=th&count=100&include_entities=1&result_type=mixed&tweet_mode=extended'

def get_hashtag( raw_query : str ) -> str:
    return re.search( r'q=%23(.+?)&', raw_query ).group(1)

def process_result( hashtag : str, tweets : list, query_metadata : dict ) -> None:
    '''
    process_result

    parameter:
    tweet : dictionary of tweet object return from search tweet
    '''

    global frontier
    global visited_hashtags
    global visited_ids
    global rev_hashtags
    global records

    valid_tweet_count = 0
    threshold = 0.05

    visited_hashtags = visited_hashtags.union( [ hashtag ] )

    if len( tweets ) == 0:
        return 

    if 'next_results' in query_metadata.keys():
        query = query_metadata['next_results'].strip('?') + '&tweet_mode=extended'
        frontier.append(query)

    for tweet in tweets:
        if tweet['id'] in visited_ids or 'media' not in tweet['entities'].keys():
            continue

        record = {
            'id' : tweet['id'],
            'created_at' : tweet['created_at'],
            'text' : tweet['full_text'],
            'retweet_count' : tweet['retweet_count'],
            'favorite_count' : tweet['favorite_count'],
            'hashtags' : [ hashtag['text'] for hashtag in tweet['entities']['hashtags'] ],
            'image_urls' : [ media['media_url'] for media in tweet['entities']['media']  if media['type'] == 'photo' ]
        } 

        records.append( record )

        visited_ids.add( tweet['id'] )

        for hashtag in record['hashtags']:
            if hashtag not in visited_hashtags and build_query( hashtag ) not in frontier:
                frontier.append( build_query( hashtag ) )

        valid_tweet_count += 1

    if valid_tweet_count / len( tweets ) >= threshold:
        rev_hashtags = rev_hashtags.union( [ hashtag ] )


def load_app() -> dict :
    '''
    load twitter application information to use api
    '''
    with open( '../env/twitter_app.json' ) as f:
        return json.load( f )

def auth_app( consumer_key : str, consumer_secret : str, access_token : str, access_token_secret : str ) -> twitter.Api:
    return twitter.Api(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token_key=access_token,
        access_token_secret=access_token_secret,
        sleep_on_rate_limit=True
    )

def collect_data( api : twitter.Api, raw_query : str ):
    search_result = api.GetSearch( raw_query= raw_query, return_json=True )
    process_result( hashtag= get_hashtag( raw_query ), tweets= search_result['statuses'], query_metadata=search_result['search_metadata'] )

def insert_data() -> None:
    global records

    client = MongoClient(
        host=os.getenv('HOST'),
        username=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD'),
        authSource=os.getenv('AUTHSOURCE'),
        authMechanism=os.getenv('AUTHMECH')
    )

    db = client.get_database( os.getenv('AUTHSOURCE') )
    db.get_collection( 'tweets' ).insert_many( records )
    
    records.clear()

if __name__ == '__main__':
    apps = load_app()

    apis = list()

    for key in apps:
        apis.append(
            auth_app( 
                consumer_key= apps[key]['API_KEY'] , 
                consumer_secret=apps[key]['API_SECRET_KEY'], 
                access_token=apps[key]['ACCESS_TOKEN'], 
                access_token_secret=apps[key]['ACCESS_TOKEN_SECRET'] 
            )
        )

    apis = cycle( apis )

    seed_hashtags = [ 'ReviewThailand', 'unseenthailand', 'amazingthailand', 'TourismAuthorityOfThailand' ]

    frontier = list(map( build_query, seed_hashtags ))

    operate_hours = 24
    end_time = time.time() + ( operate_hours * 60 * 60 )

    while len( frontier ) > 0 and time.time() < end_time:
        if len(records) > 1000:
            insert_data()

        raw_query = frontier.pop( 0 )
        api = next( apis )

        collect_data( api, raw_query )

    insert_data()

    with open('rev_hashtags.json','w') as f:
        json.dump( list(rev_hashtags), f )

    with open('visited_ids.json','w') as f:
        json.dump( list(visited_ids), f )

    with open('frontier.json','w') as f:
        json.dump( frontier, f )

