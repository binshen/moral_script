from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient('121.40.92.176', 27017)
db = client.moral_db

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
print yesterday
daily_data = db.data.aggregate([
    {'$match':{'day':yesterday }},
    {'$group':{
        '_id':'$mac',
        'x1':{'$avg':'$x1'},
        'x3':{'$avg':'$x3'},
        'x9':{'$avg':'$x9'},
        'x10':{'$avg':'$x10'},
        'x11':{'$avg':'$x11'}
    }}
])

datalist = []
for d in list(daily_data):
    datalist.append({
        'mac':d['_id'],
        'day':yesterday,
        'x1':round(d['x1'], 0),
        'x3':round(d['x3'], 0),
        'x9':round(d['x9'], 0),
        'x10':round(d['x10'], 0),
        'x11':round(d['x11'], 0)
    })
    if len(datalist) == 100:
        db.data_daily.insert_many(datalist)
        datalist = []

if len(datalist) > 0:
    db.data_daily.insert_many(datalist)

client.close()