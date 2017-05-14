import json
import uuid
from chance import chance
from random import randint
import argparse

def document():
    doc = {
        '_id': uuid.uuid4().hex, 
        '_rev': uuid.uuid4().hex,
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
parser.add_argument("count", help="number of generated sql insert statements.", type=int)
args = parser.parse_args()

i = args.count
print("BEGIN TRANSACTION;")
batch = []
while i>0:
    print("INSERT INTO TEST VALUES (json('{}'));".format(json.dumps(document())))
    i -= 1
print('COMMIT;')

# CREATE INDEX firstname ON test (json_extract(body, "$.firstname"));
# CREATE INDEX lastname ON test (json_extract(body, "$.lastname"));
# CREATE INDEX imdb ON test (json_extract(body, "$.rating.imdb"));
# CREATE INDEX rottentomatoes ON test (json_extract(body, "$.rating.rottentomatoes"));
# CREATE INDEX empire ON test (json_extract(body, "$.rating.empire"));
# CREATE INDEX totalfilm ON test (json_extract(body, "$.rating.totalfilm"));
# CREATE INDEX guardian ON test (json_extract(body, "$.rating.guardian"));
# CREATE INDEX email ON test (json_extract(body, "$.email"));