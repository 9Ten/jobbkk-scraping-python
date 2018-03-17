# -*- coding: utf-8 -*-
import hashlib
import pandas as pd
import config

#=== mongoDB Config ===#
def connect_db():
    try:
        conn = MongoClient(
            host=config.DATABASE_CONFIG['host'],
            port=config.DATABASE_CONFIG['port'],
        )
    except Exception as e:
        print('Error Connection')
    return conn

pp = pprint.PrettyPrinter(indent=4)
dbname = config.DATABASE_CONFIG['dbname']
col_bkk_job_info = conn[dbname]['job_info']
col_bkk_resume_info = conn[dbname]['resume_info']
col_bkk_update = conn[dbname]['bkk_updadte']
col_bkk_log = conn[dbname]['bkk_log']


def hash_key(word):
    m = hashlib.md5()
    try:
        m.update(str(word).encode('utf-8'))
        return m.hexdigest()
    except:
        return None

        
def job_info_to_csv():
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
    cursor = col_bkk_resume_info.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(list(cursor))
    df.to_csv('job.csv')
    print("===== Done CSV =====")


def resume_info_to_csv():
    pipeline = [

    ]
    cursor = col_bkk_job_info.aggreagate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(list(cursor))
    df.to_csv('resume.csv')
    print("===== Done CSV =====")
