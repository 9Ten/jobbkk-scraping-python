# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import multiprocessing
import pprint
import datetime
import re
import time

from collections import OrderedDict
from pymongo import MongoClient
import pymongo
import json
import config

#=== mongoDB Config ===#


def connect_db():
    try:
        conn = MongoClient(
            host=config.DATABASE_CONFIG['host'],
            port=config.DATABASE_CONFIG['port'],
        )
    except Exception as e:
        print('Error Connection', e)
    return conn


conn = connect_db()
dbname = config.DATABASE_CONFIG['dbname']
col_bkk_job_info = conn[dbname]['job_info']
col_bkk_update = conn[dbname]['bkk_updadte']
col_bkk_log = conn[dbname]['bkk_log']

pp = pprint.PrettyPrinter(indent=4)
header = config.header
base_url = "https://www.jobbkk.com"
occupation_url = '/หางาน กฎหมาย,ทั้งหมด,ทุกจังหวัด,ทั้งหมด.html?occupation_id='

#=== Initial const ====#
occupation_dict = config.occupation_mapper
edu_dict = config.edu_lvl_mapper


def retry(url):
    chk_status = -1
    while chk_status != 200:
        try:
            res = requests.get(url, headers=header, timeout=10)
            time.sleep(0.1)
            chk_status = res.status_code
        except Exception as e:
            print("===== RETRY =====")
            print(e)
            print("Continue ...")
            continue
    return res


def check_id(url):
    job_id = re.search(r'\d+/\d+', url).group(0).split("/")
    if len(list(col_bkk_job_info.find({'_id': int(job_id[1])}).limit(1))) > 0:
        return None
    else:
        return url


def job_indexer(intial_url, update_page, oid):
    #=== Looping occupation_id ===#
    occupation_list = list(occupation_dict.keys())
    index = occupation_list.index(oid)
    for _id in occupation_list[index:]:
        url = intial_url + str(update_page) + occupation_url + str(_id)
        res = retry(url)
        soup = BeautifulSoup(res.content, 'lxml')
        try:
            total_page = int(soup.select_one(
                "div.row-fluid.tdF span.colRed").text.replace(',', ''))
        except:
            total_page = 0

        #=== Initial page ===#
        job_page_list(url, _id)
        print("========== Extract job@oid: {} | item: {} ==========".format(
            _id, total_page))

        # #=== Looping next page ===#
        if total_page > 0:
            for page in range(update_page + 1, (total_page // 25 + 2)):
                url = intial_url + str(page) + occupation_url + str(_id)
                job_page_list(url, _id)
                col_bkk_update.update_one({'_id': 'dump_job_info'}, {
                    '$set': {'occupation_id': _id, 'next_page': page, 'date_cal': datetime.datetime.now()}}, upsert=True)
        update_page = 1


def job_page_list(url, _id):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        job_list = soup.select_one("div.pad5").select("div.jsearchCon h6 a")
    except TypeError:
        print("job_page_list")
    #=== Looping extract job_id ===#
    job_link_list = [_['href'] for _ in job_list]
    job_link_list = [check_id(_) for _ in job_link_list]
    job_link_list = [_ for _ in job_link_list if _ != None]

    #=== Parallel [multiprocessing] ===#
    print("===== TODO Parallel =====")
    pool = multiprocessing.Pool(4)
    results = [pool.apply_async(job_page, (url, _id), )
               for url in job_link_list]
    output = [_.get() for _ in results]
    output = [_ for _ in output if _ != 0]
    if len(output) > 0:
        pool.close()
        pool.join()
        #=== TODO Insert db ===#
        try:
            col_bkk_job_info.insert_many(output, ordered=False)
        except Exception as e:
            print("Error insert db", e)
    else:
        pool.terminate()
        pool.join()
    print("===== TASK DONE =====")


def job_page(url, _id):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    #=== Extract Json ===#
    try:
        data_dict = json.loads(soup.find_all(
            'script', {"type": "application/ld+json"})[1].text, strict=False)
        job_title = data_dict['title']
        description = data_dict['description']
        company = data_dict['hiringOrganization']['name']
        job_id = data_dict['hiringOrganization']['sameAs']
        job_id = re.search(r'\d+/\d+', job_id).group(0).split("/")
        date_post = data_dict['datePosted']
    except Exception as e:
        print("Step job_json_data", e)
        return 0

    #=== Extract Static JobBKK ===#
    try:
        static_detail_list = [_.text.strip()
                              for _ in soup.select("div.statis-detail")]
    except Exception as e:
        pass
        # print("Step job_static", e)

    #=== Extract Interesting ===#
    try:
        applicants = soup.select_one("#loadnumapply").text.strip()
    except Exception as e:
        pass
        # print("Step job_interesting", e)

    #=== Extract Info ===#
    try:
        info = soup.select_one("div.row-left")
        detail_list = [_.text.strip() for _ in info.select_one(
            "div.job-detail.border-b").select("span")]
        skill_list = [_.text.strip() for _ in info.select_one(
            "div[itemprop=skills]").select("span")]
        incentives_detail_list = [_.text.strip() for _ in info.select_one(
            "div[itemprop=incentives]").select("li")]
        incentives_additional = info.select_one(
            "div[itemprop=incentives] div").text.strip()
    except Exception as e:
        pass
        # print("Step job_info", e)

    #=== Extract Transport ===#
    try:
        jobLocation = info.select_one("div[itemprop=jobLocation]")
        transport_detail_list = [_.text.strip().replace(
            'ไม่มี', '') for _ in jobLocation.select("div.transport-detail")]
    except Exception as e:
        pass
        # print("Step job_transport", e)

    #=== Extract Main Info ===#
    if re.search('-', skill_list[2]) != None:
        edu_clean = skill_list[2].split('-')
        edu_clean = edu_dict[edu_clean[0].strip()] + '-' + \
            edu_dict[edu_clean[1].strip()]
    else:
        try:
            edu_clean = edu_dict[edu_clean]
        except NameError:
            edu_clean = ""
            pass
    try:
        job_dict = OrderedDict({
            '_id': int(job_id[1]),
            'occupation_id': _id,
            'job_title': job_title,
            'job_description': description.replace('\n', '|'),
            'num_position': float(detail_list[0].replace('ตำแหน่ง', '').replace('ไม่ระบุ', '0').replace('ไม่จำกัด', 'Inf')),
            'job_type': detail_list[1],
            'company_id': int(job_id[0]),
            'company_name': company,
            'company_location': {
                #=== Location Company ===#
                'street_address': data_dict['jobLocation']['address']['streetAddress'],
                'local_address': data_dict['jobLocation']['address']['addressLocality'],
                'region_address': data_dict['jobLocation']['address']['addressRegion'],
                'postal_code': data_dict['jobLocation']['address']['postalCode'],
                'country_address': data_dict['jobLocation']['address']['addressCountry']
            },
            'work_location': detail_list[2].split(','),
            'salary': detail_list[3].replace(',', '').replace(' ', ''),
            'vacation': detail_list[5].replace('ไม่ระบุ', ''),
            'work_time': detail_list[4].replace('ไม่ระบุ', ''),
            'gender': skill_list[0].replace(' ', '').replace('ชาย', 'M').replace('หญิง', 'F').replace(',', ''),
            'age': skill_list[1].replace('ปีขึ้นไป', '+').replace('ทุกช่วงอายุ', '').replace(' ', ''),
            'edu': edu_clean,
            'exp': skill_list[3].replace('ปีขึ้นไป', '+').replace(' ', ''),
            'other': skill_list[4].replace('ไม่ระบุ', ''),
            'incentives': incentives_detail_list,
            'incentives_add': incentives_additional,
            'transport': {
                'bus': transport_detail_list[0],
                'bts': transport_detail_list[1],
                'mrt': transport_detail_list[2],
                'arl': transport_detail_list[3]
            },
            'applicants': int(applicants.replace(',', '')),
            'job_active': static_detail_list[1],
            'job_view': int(static_detail_list[0].replace(',', '')),
            'job_date_post': date_post,
        })
    except Exception as e:
        print("Step job_dict", e)
    return job_dict


def checks_update():
    try:
        cursor = list(col_bkk_update.find(
            {'_id': 'dump_job_info'}).limit(1))[0]
        return cursor['next_page'], cursor['occupation_id']
    except:
        return 1, 238


if __name__ == '__main__':
    start = datetime.datetime.now()
    print("Start time: {}".format(start))

    #=== Initial url ===#
    update_page, oid = checks_update()
    print("===== Start extract jobbkk@: {}, oid: {} =====".format(update_page, oid))
    init_job_url = "https://www.jobbkk.com/jobs/lists/"

    #=== TODO Insert ===#
    job_indexer(init_job_url, update_page, oid)

    #=== TODO Update ===#
    # Function update job exists

    end = datetime.datetime.now()
    col_bkk_update.update_one({'_id': 'dump_job_info'}, {
                              '$set': {'date_cal': end}}, upsert=True)
    print("End time: {}".format(end))
    print("Cost time: {}".format(end - start))
