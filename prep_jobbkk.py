# -*- coding: utf-8 -*-
import hashlib
import pandas as pd
import config

#=== mongoDB Config ===#
global col_bkk_job
global col_bkk_resume
try:
    conn = MongoClient(
        host='localhost',
        port=27017
    )
except:
    print("===== Error connect =====")

col_bkk_job = conn[dbname]['job_info']
col_bkk_resume = conn[dbname]['resume_info']
col_bkk_update = conn[dbname]['bkk_updadte']
col_bkk_log = conn[dbname]['bkk_log']

pp = pprint.PrettyPrinter(indent=4)
dbname = config.DATABASE_CONFIG[dbname]

def connect_db():
    try:
        conn = MongoClient(
            host=config.DATABASE_CONFIG['host'],
            port=config.DATABASE_CONFIG['port'],
        )
    except Exception as e:
        print('Error Config {}'.format(dbname))
    return conn


def hash_key(word):
    m = hashlib.md5()
    try:
        m.update(str(word).encode('utf-8'))
        return m.hexdigest()
    except:
        return None

        
def collection_to_csv():
    pipeline = [
        {'$project': {'resume_id': 1, 'edu_hist': 1}},
        {'$limit': 1000},
        {'$unwind': "$edu_hist"},
        {'$project': {
            '_id': 0,
            'resume_id': 1,
            'edu_year': "$edu_hist.edu_year",
            'edu_location': "$edu_hist.location",
            'edu_institute': "$edu_hist.institute",
            'edu_year': "$edu_hist.edu_year",
            'edu_level': "$edu_hist.edu_level",
            'edu_edu': "$edu_hist.edu_edu",
        }}
    ]
    cursor = col_bkk_job.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(list(cursor))
    df.to_csv('job.csv')
    print("===== Done CSV =====")
