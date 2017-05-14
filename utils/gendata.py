import json
import uuid
from chance import chance
from random import randint
import argparse
from sovoc.sovoc import Sovoc

def document():
    doc = {
        'description': chance.paragraph(),
        'date': chance.date(year=0, month=0, day=0, hour=0, minutes=0, minyear=1985).strftime("%Y-%m-%d %H:%M:%S"),
        'firstname': chance.first(),
        'lastname': chance.last(),
        'email': chance.email(),
        'ip': chance.ip(),
        'rating': {
            'imdb': randint(0,9),
            'rottentomatoes': randint(0,9),
            'empire': randint(0,9),
            'totalfilm': randint(0,9),
            'guardian': randint(0,9)
        },
        'data': [randint(0,9) for _ in range(0, 10)]
    }

    return doc


parser = argparse.ArgumentParser("random doc generator")
parser.add_argument("count", help="number of generated documents.", type=int)
args = parser.parse_args()
db = Sovoc('bigdata.db')
db.setup()

i = args.count
batch = []
while i>0:
    if len(batch) == 1000:
        db.bulk(batch)
        batch = []

    batch.append(document())
    i -= 1

if len(batch) > 0:
    db.bulk(batch)