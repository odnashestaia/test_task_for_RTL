import calendar
from datetime import datetime, timedelta
from datetime import datetime
import os

from pymongo import MongoClient
from dotenv import load_dotenv

def get_aggregated_data(dt_from, dt_upto, group_type):

    # подсоединение к бд
    load_dotenv()
    DB = os.getenv('DB')
    client = MongoClient(DB)
    db = client['test']
    collection = db['sample_collection']

    """ типы груперровки в нужный формат """
    group_types = {
        'hour': '%Y-%m-%dT%H',
        'day': '%Y-%m-%d',
        'month': '%Y-%m'
    }

    iso_types = {
        'hour': ':00:00',
        'day': 'T00:00:00',
        'month': '-01T00:00:00'
    }


    dt_format = group_types[group_type]

    """ делаем запрос """
    cursor = collection.aggregate([
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {"$dateToString": {"format": dt_format, "date": "$dt"}},
            "sum_value": {"$sum": '$value'}}},
        {"$sort": {"_id": 1}},
    ])

    
    labels = []
    data = []

    iso_format = iso_types[group_type]

    for doc in cursor:
        dt_raw = datetime.fromisoformat(doc['_id'] + iso_format)
        dt_iso = datetime.isoformat(dt_raw)
        labels.append(dt_iso)
        data.append(doc['sum_value'])

    result_data = []
    result_labels = []

    current_date = dt_from

    while current_date <= dt_upto:
        
        if group_type == 'hour':
            delta = timedelta(hours=1)
        elif group_type == 'day':
            delta = timedelta(days=1)
        elif group_type == 'month':
            _, days_in_month = calendar.monthrange(current_date.year, current_date.month)
            delta = timedelta(days=days_in_month)

        result_labels.append(datetime.isoformat(current_date))

        if datetime.isoformat(current_date) not in labels:
            result_data.append(0)
        else:
            value_index = labels.index(datetime.isoformat(current_date))
            result_data.append(data[value_index])

        current_date += delta

    return {'dataset': result_data, 'labels': result_labels}
