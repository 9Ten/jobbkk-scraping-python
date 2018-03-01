# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import hashlib
import multiprocessing
import os
import pprint
import datetime
import re
import time

from collections import Counter, OrderedDict
import json
from pymongo import MongoClient
import pymongo
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
col_bkk_job = conn['jobBkk']['job_info']
col_bkk_resume = conn['jobBkk']['resume_info']
col_bkk_update = conn['jobBkk']['bkk_updadte']
col_bkk_log = conn['jobBkk']['bkk_log']

#=== For Job Analysis ===#
pp = pprint.PrettyPrinter(indent=4)
header = config.header
base_url = "https://www.jobbkk.com"
occupation_url = '/หางาน กฎหมาย,ทั้งหมด,ทุกจังหวัด,ทั้งหมด.html?occupation_id='

#=== Initial const ====#
occupation_dict = config.occupation_dict
lang_mapper = config.lang_mapper
edu_dict = config.edu_dict
edu_mapper = config.edu_mapper
exp_mapper = config.exp_mapper
train_mapper = config.train_mapper


def hash_key(word):
    m = hashlib.md5()
    try:
        m.update(str(word).encode('utf-8'))
        return m.hexdigest()
    except:
        return None


def retry(url):
    chk_status = -1
    while chk_status != 200:
        try:
            res = requests.get(url, headers=header, timeout=10)
            time.sleep(0.25)
            chk_status = res.status_code
        except Exception as e:
            print("===== RETRY =====")
            print(e)
            print("Continue ...")
            continue
    return res


def check_id(url):
    try:
        resume_id = int(re.search('\/\d+\/', url).group(0).replace('/', ''))
    except:
        print("key: {}".format(url))
        return 0
    if list(col_bkk_resume.find({'_id': resume_id}).limit(1)) > 0:
        return 0
    else:
        return 1


def resume_indexer(intial_url, update_page):
    date_cal = datetime.datetime.now()
    url = intial_url + str(update_page)
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    total_page = int(soup.select_one(
        "div#total_resume_row span.colRed").text.replace(',', ''))
    #=== Initial page ===#
    resume_page_list(intial_url)
    print("========== Extract resume_indexer items: {} ==========".format(total_page))

    """
    Multiprocessing

    """
    #=== Looping next page ===#
    url_buffer = []
    for page in range(update_page + 1, total_page // 15):
        url = intial_url + str(page)
        pool_url.append(url)

    pool = multiprocessing.Pool(4)
    result = pool.map_async(resume_page_list, url_buffer, chunksize=4)
    pool.close()
    pool.join()

    col_bkk_update.update_one({'type': 'dump_resume_info'}, {'$set': {'next_page': page, 'date_cal': date_cal}}, upsert=True)


def job_indexer(intial_url, update_page):
    #=== Looping occupation_id ===#
    for _id in occupation_dict.keys():
        url = intial_url + str(update_page) + occupation_url + str(_id)
        res = retry(url)
        soup = BeautifulSoup(res.content, 'lxml')
        total_page = int(soup.select_one(
            "div.row-fluid.tdF span.colRed").text.replace(',', ''))
        #=== Initial page ===#
        job_page_list(url, _id)
        print("========== Extract job@oid: {} | item: {} ==========".format(
            _id, total_page))

        """
        Multiprocessing
        """
        #=== Looping next page ===#
        for page in range(update_page + 1, total_page // 25):
            url = intial_url + str(page) + occupation_url + str(_id)
            job_page_list(url, _id)
            col_bkk_update.update_one({'type': 'dump_resume_info'}, {
                                      '$set': {'next_page': page, 'date_cal': date_cal}}, upsert=True)


def resume_page_list(url):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        resume_list = soup.select_one("div.pad5").select(
            "div.span5 a.checkClicklist")
    except TypeError:
        print("resume_page_list")
    #=== Looping Extract resume_id ===#
    resume_link_list = [_['href'] for _ in resume_list]
    resume_link_list = map(lambda link: check_id(link), resume_link_list)
    resume_link_list = filter(lambda link: link > 0, resume_link_list)

    #=== Parallel [multiprocessing] ===#
    print("===== TODO Parallel =====")
    pool = multiprocessing.Pool(4)
    result = pool.map_async(resume_page, resume_link_list, chunksize=4)
    result = filter(lambda res: res != 0, result.get())
    if len(result) > 0:
        pool.close()
        pool.join()
        #=== TODO Insert db ===#
        try:
            col_bkk_resume.insert_many(result, ordered=False)
        except Exception as e:
            print("Error insert db", e)
    else:
        pool.terminate()
        pool.join()
    print("===== TASK DONE =====")


def job_page_list(url, _id):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        job_list = soup.select_one("div.pad5").select("div.jsearchCon h6 a")
    except TypeError:
        print("job_page_list")
    #=== Looping extract job_id ===#
    job_url_list = [_['href'] for _ in job_list]



    #=== Parallel [multiprocessing] ===#
    print("===== TODO Parallel =====")
    pool = multiprocessing.Pool(4)
    results = [pool.apply_async(job_page, (url, _id), )
               for url in job_url_list]
    output = [_.get() for _ in results]
    output = filter(lambda res: res != 0, output)
    if len(output) > 0:
        pool.close()
        pool.join()
        #=== TODO Insert db ===#
        try:
            col_bkk_job.insert_many(output, ordered=False)
        except Exception as e:
            print("Error insert db", e)
    else:
        pool.terminate()
        pool.join()
    print("===== TASK DONE =====")


def job_page(url, _id):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    #=== Extract Static JobBKK ===#
    try:
        static_detail_list = [_.text.strip()
                              for _ in soup.select("div.statis-detail")]
    except Exception as e:
        print("Step job_static", e)

    #=== Extract Interesting ===#
    try:
        applicants = soup.select_one("#loadnumapply").text.strip()
    except Exception as e:
        print("Step job_interesting", e)

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
        print("Step job_info", e)

    #=== Extract Transport ===#
    try:
        jobLocation = info.select_one("div[itemprop=jobLocation]")
        transport_detail_list = [_.text.strip().replace(
            'ไม่มี', '') for _ in jobLocation.select("div.transport-detail")]
        # transport_additional = jobLocation.select_one("div.transport-additional span").text.strip()
    except Exception as e:
        print("Step job_transport", e)

    #=== Extract Json ===#
    try:
        data_dict = json.loads(soup.find_all(
            'script', {"type": "application/ld+json"})[1].text, strict=False)
        job_title = data_dict['title']
        description = data_dict['description']
        company = data_dict['hiringOrganization']['name']
        job_com_id = data_dict['hiringOrganization']['sameAs']
        job_com_id = re.search('\d+/\d+', job_com_id).group(0).split("/")
        date_post = data_dict['datePosted']
    except Exception as e:
        print("Step job_json_data", e)

    #=== Extract Main Info ===#
    if re.search('-', skill_list[2]) != None:
        edu_clean = skill_list[2].replace(' ', '').split('-')
        edu_clean = edu_dict[edu_clean[0]] + \
            '-' + edu_dict[edu_clean[1]]
    else:
        try:
            edu_clean = edu_dict[edu_clean]
        except KeyError:
            print("Step KeyError: ", edu_clean)
            edu_clean = ""
    try:
        job_dict = OrderedDict({
            'occupation_id': _id,
            'job_id': int(job_com_id[1]),
            'job_title': job_title,
            'job_description': description.replace('\n', '|'),
            'num_position': int(detail_list[0].replace('ตำแหน่ง', '').replace('ไม่ระบุ', '').replace('ไม่จำกัด', 'Inf').strip()),
            'job_type': detail_list[1],
            'company_id': int(job_com_id[0]),
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
            'edu': edu_clean.strip(),
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
            'applicants': int(applicants),
            'job_active': static_detail_list[1],
            'job_view': int(static_detail_list[0].replace(',', '')),
            'job_date_post': date_post,
        })
    except Exception as e:
        print("Step job_dict", e)
    # try:
    #     col_bkk_job.insert_one(job_dict)
    # except Exception as e:
    #     print('db', e)
    return job_dict


def resume_page(url):
    #=== Check Key ===#
    resume_id = int(re.search('\/\d+\/', url).group(0).replace('/', ''))
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        #=== Check Not Found ===#
        info_sum = soup.select_one(
            "div#box_right div.span11.marL10.padB10.padL20")
        if info_sum == None:
            return 0
        #=== Extract Info Summary ===#
        prog = info_sum.select_one("div#progress_resume div")[
            'style'].strip()[-3:].replace('%', '')
        [div.decompose() for div in info_sum.select("div")]
        [b.decompose() for b in info_sum.select("b")]
        info_sum_list = re.sub(
            '[\s+]', '|', info_sum.text.strip().replace(" ", "")).split("|")
        info_sum_list = list(filter(lambda _: _ != "", info_sum_list))

        #=== Extract Info Resume ===#
        info_main = soup.select_one("div#resumeDT")
        resume_update = info_main.select_one(
            "div.taR.marR10").text.strip().split(":")

        resume_want_dict = {
            'job_type': '',
            'asked_job': [],
            'asked_location': '',
            'asked_salary': '',
            'start_in': '',
            'work_aboard': ''
        }
        resume_want_mapper = {
            'รูปแบบงาน:': 'job_type',
            'สาขาอาชีพ:': 'asked_job',
            'ตำแหน่ง:': 'asked_job',
            'พื้นที่ที่ต้องการทำงาน:': 'asked_location',
            'เงินเดือนที่ต้องการ:': 'asked_salary',
            'ระยะเวลาเริ่มงาน:': 'start_in',
            'ยินดีทำงานต่างประเทศ:': 'work_aboard'
        }
        resume_want = info_main.select_one(
            "div#resume_want").select("div.span11.offset1")
        if len(resume_want) > 0:
            for row in resume_want:
                # pair {occupation, position}
                op_dict = {}
                for col in row.select('.span6'):
                    key = re.sub('[\d.\s+]', '', col.b.text)
                    val = re.sub('[\s+]', '', col.span.text)
                    key = resume_want_mapper[key]
                    if key == 'asked_job':
                        if len(op_dict) == 0:
                            op_dict['occupation'] = val
                        else:
                            op_dict['position'] = val
                            resume_want_dict[key].append(op_dict)
                            op_dict = {}
                    else:
                        resume_want_dict[key] = val
    except Exception as e:
        print("Step info_resume", e)

    #=== Extract Education ===#
    try:
        resume_edu = []
        for dl in info_main.select("dl"):
            key = dl.select_one("dt").text.strip().replace(" :", "")
            val = dl.select_one("dd")
            for span in val.select("span"):
                span.decompose()
            val = re.sub('[\s+]', '|', val.text.strip().replace(" ", ""))
            val_list = list(filter(lambda _: _ != "", val.split("|")))
            resume_edu.append(
                (key, val_list[1:3] + val_list[4:])
            )
    except Exception as e:
        print("Step resume_edu", e)

    #=== Extract Experience ===#
    try:
        resume_exp = info_main.select("div.row-fluid.jsXp_row.padB5")
        if resume_exp != None:
            resume_exp_list = []
            for exp in resume_exp:
                work_info_list = [re.sub('[\s+]', ' ', _.text.strip()) for _ in exp.select_one(
                    "div.o.col000.span6.padV10H20.cor4.bg_lightyellow").select("span.padL10")]
                work_detail = exp.select_one("div.padB10.bb-code").text.strip()
                resume_exp_list.append((work_info_list, work_detail))
    except Exception as e:
        print("Step resume_exp", e)

    #=== Extract Skill[Nosql] ===#
    try:
        resume_skill_dict = {
            'own_vehicle': '',
            'skill_vehicle': '',
            'drive_license': '',
            'skill_lang': [],
            'skill_typing': {'th_wm': '', 'en_wm': ''},
            'skill_other': ''
        }
        resume_skill_mapper = {
            'ยานพาหนะ': 'own_vehicle',
            'ความสามารถในการขับขี่': 'skill_vehicle',
            'ใบอนุญาติขับขี่': 'drive_license',
            'ทักษะทางภาษา': 'skill_lang',
            'ทักษะการพิมพ์ดีด': 'skill_typing',
            'ทักษะอื่นๆ': 'skill_other'
        }
        resume_skill = info_main.select_one("div#resume_skill")
        lang_list = []
        if resume_skill != None:
            for skill_soup in resume_skill.select("div.padV10H20 > div.span11.offset1"):
                # skill_lang
                try:
                    skill_soup['style']
                except KeyError:
                    skill_soup['style'] = None
                if skill_soup['style'] == "float:left":
                    for row in skill_soup.select('div.span11.offset1'):
                        key = row.select_one(
                            '.span2.bg_lightyellow.taCen.o').text.strip()
                        val_list = [lang_mapper[re.sub(
                            '[\s+]', '', _.text).split(':')[1]] for _ in row.select('.pull-left')]
                        resume_skill_dict['skill_lang'].append(
                            {
                                'name': key,
                                'skill': {
                                    "listen": val_list[0],
                                    "speak": val_list[1],
                                    "read": val_list[2],
                                    "write": val_list[3]
                                }
                            }
                        )
                else:
                    # pair {skill_key, skill_val}
                    try:
                        key, val = re.sub(
                            '[\s+]', '', skill_soup.text).split(':')
                    except:
                        # skill_other too many values
                        continue
                    key = resume_skill_mapper[key]
                    if key == 'skill_typing':
                        val_list = re.findall('\d+', val)
                        try:
                            resume_skill_dict[key]['th_wm'] = val_list[0]
                        except:
                            pass
                        try:
                            resume_skill_dict[key]['en_wm'] = val_list[1]
                        except:
                            pass
                    else:
                        resume_skill_dict[key] = val
    except Exception as e:
        print("Step resume_skill", e)

    #=== Extract Main Info ===#
    try:
        # Handle cast String into Integer
        try:
            age = int(info_sum_list[1])
        except:
            age = ''
        try:
            asked_salary = int(
                resume_want_dict['asked_salary'].replace(',', ''))
        except:
            asked_salary = ''
        resume_csv_dict = OrderedDict({
            # Primary key
            '_id': resume_id,
            'resume_modified': resume_update[1].strip(),
            'resume_progress': int(prog),
            'gender': info_sum_list[0].replace('ชาย', 'M').replace('หญิง', 'F'),
            'age': age,
            'exprience': int(info_sum_list[2]),
            'job_type': resume_want_dict['job_type'],
            'asked_job': resume_want_dict['asked_job'],
            'asked_location': resume_want_dict['asked_location'],
            'asked_salary': asked_salary,
            'start_in': resume_want_dict['start_in'],
            'work_aboard': resume_want_dict['work_aboard'],
            'edu_hist': [],
            'exp_hist': [],
            'own_vehicle': resume_skill_dict['own_vehicle'],
            'skill_vehicle': resume_skill_dict['skill_vehicle'],
            'drive_license': resume_skill_dict['drive_license'],
            'skill_lang': resume_skill_dict['skill_lang'],
            'skill_typing': resume_skill_dict['skill_typing'],
            'skill_other': resume_skill_dict['skill_other'],
            'training_hist': []
        })
    except Exception as e:
        print("Step resume_csv", e)

    #=== Extract exp ===#
    try:
        if len(resume_exp_list) > 0:
            for rel in resume_exp_list:
                result_dict = {}
                for exp, _ in zip(exp_mapper, rel[0]):
                    result_dict[exp] = _
                resume_csv_dict['exp_hist'].append(
                    {'exp_info': result_dict, 'exp_detail': rel[1]})
    except Exception as e:
        print("Step exp_csv", e)

    #=== Extract education ===#
    try:
        if len(resume_edu) > 0:
            for redu in resume_edu:
                result_dict = {'edu_year': redu[0].replace(
                    'กำลังศึกษาอยู่', 'Studying')}
                for edu, r in zip(edu_mapper, redu[1]):
                    if edu == "edu_level":
                        try:
                            r = edu_dict[r]
                        except KeyError:
                            # print("KeyError: ", r)
                            r = edu_dict[r.replace(
                                '(กำลังศึกษาอยู่)', '')]
                            r = r + ' (Studying)'
                    result_dict[edu] = r
                resume_csv_dict['edu_hist'].append(result_dict)
    except Exception as e:
        print("Step edu_csv", e)

    #=== Extract training ====#
    try:
        training_list = info_main.select_one(
            "#resume_skill + div").select("div.row-fluid")
        if training_list != None:
            for train in training_list:
                result_dict = {}
                tl = [re.sub('[\s+]', ' ', _.text.strip())
                      for _ in train.select("span")]
                for tm, td in zip(train_mapper, tl):
                    result_dict[tm] = td
                if len(result_dict) == 0:
                    break
                else:
                    resume_csv_dict['training_hist'].append(result_dict)
    except Exception as e:
        print("Step training_csv", e)
    return resume_csv_dict


def checks_update():
    col_bkk_update = conn['jobBkk']['dump_bkk_updadte']
    cursor = list(col_bkk_update.find(
        {'type': 'dump_resume_info'}).limit(1))[0]
    next_page = cursor['next_page']
    return next_page


def collection_to_csv():
    col_bkk_resume = conn['jobBkk']['resume_info']
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
    cursor = col_bkk_resume.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(list(cursor))
    df.to_csv('job.csv')
    print("===== Done CSV =====")


if __name__ == '__main__':
    start = datetime.datetime.now()
    print("Start time: {}".format(start))

    #=== Initial url ===#
    # update_page = checks_update()
    update_page = 1
    print("===== Start extract jobbkk@: {} =====".format(update_page))
    init_resume_url = "https://www.jobbkk.com/resumes/lists/"
    init_job_url = "https://www.jobbkk.com/jobs/lists/"

    #=== TODO Insert ===#
    resume_indexer(init_resume_url, update_page)
    # job_indexer(init_job_url, update_page)

    #=== TODO Update ===#
    # Function update resume exists

    end = datetime.datetime.now()
    print("End time: {}".format(end))
    print("Cost time: {}".format(end - start))
