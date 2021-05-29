import os
from itertools import product

from pymongo.database import Database
from pymongo import MongoClient
from dotenv import load_dotenv
import networkx as nx

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

def create_graph( step : int ) -> nx.Graph:
    roots = { 'ReviewThailand', 'unseenthailand', 'amazingthailand', 'TourismAuthorityOfThailand' }

    db = connect()

    next_hop = roots
    visited = set()

    G = nx.Graph()

    for i in range(step):
        edges = set()

        cursor = db.get_collection( 'tweets' ).find({
            'hashtags' : { '$in' : list(next_hop) }
        }, no_cursor_timeout=True)

        visited = visited.union( next_hop )
        next_hop.clear()

        for record in cursor:
            next_hop = next_hop.union( record['hashtags'] )
            edges = edges.union( product( record['hashtags'], record['hashtags'] ) )
        
        cursor.close()

        next_hop.difference( visited )

        G.add_edges_from( list(edges) )

    return G

if __name__ == '__main__':
    G = create_graph( 10 )
    nx.write_adjlist( G, 'graph.adjlist' )
