import os
import json

from pymongo.database import Database
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv('../env/mongo_auth.env')

def connect() -> Database:
    client = MongoClient(
        host=os.getenv('HOST'),
        username=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD'),
        authSource=os.getenv('AUTHSOURCE'),
        authMechanism=os.getenv('AUTHMECH')
    )

    db = client.get_database( os.getenv('AUTHSOURCE') )
    return db

def create_graph( step : int ) -> set:
    roots = { 'ReviewThailand', 'unseenthailand', 'amazingthailand', 'TourismAuthorityOfThailand' }

    db = connect()

    next_hop = roots
    visited = set()

    for i in range(step):
        cursor = db.get_collection( 'tweets' ).find({
            'hashtags' : { '$in' : list(next_hop) }
        }, no_cursor_timeout=True)

        visited = visited.union( next_hop )
        next_hop.clear()

        for record in cursor:
            next_hop = next_hop.union( record['hashtags'] )
        
        cursor.close()

        next_hop.difference( visited )

    return visited

if __name__ == '__main__':
    res = create_graph( 10 )

    with open('result.json','w') as f:
        json.dump( list(res), f )
