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
col_bkk_resume_info = conn[dbname]['resume_info']
col_bkk_update = conn[dbname]['bkk_updadte']
col_bkk_log = conn[dbname]['bkk_log']

pp = pprint.PrettyPrinter(indent=4)
header = config.header
base_url = "https://www.jobbkk.com"

#=== Initial const ====#
lang_mapper = config.lang_mapper
edu_dict = config.edu_lvl_mapper
edu_mapper = config.edu_mapper
exp_mapper = config.exp_mapper
train_mapper = config.train_mapper
resume_want_mapper = config.resume_want_mapper
resume_skill_mapper = config.resume_skill_mapper


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
    resume_id = int(re.search(r'\/\d+\/', url).group(0).replace('/', ''))
    if len(list(col_bkk_resume_info.find({'_id': resume_id}).limit(1))) > 0:
        return None
    else:
        return url


def resume_indexer(intial_url, update_page):
    url = intial_url + str(update_page)
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    total_page = int(soup.select_one(
        "div#total_resume_row span.colRed").text.replace(',', ''))
    #=== Initial page ===#
    resume_page_list(intial_url)
    print("========== Extract resume_indexer items: {} ==========".format(total_page))

    #=== Looping next page ===#
    for page in range(update_page + 1, (total_page // 15 + 2)):
        url = intial_url + str(page)
        resume_page_list(url)
        col_bkk_update.update_one({'_id': 'dump_resume_info'}, {
                                  '$set': {'next_page': page, 'date_cal': datetime.datetime.now()}}, upsert=True)


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
    resume_link_list = [check_id(_) for _ in resume_link_list]
    resume_link_list = [_ for _ in resume_link_list if _ != None]

    #=== Parallel [multiprocessing] ===#
    print("===== TODO Parallel =====")
    pool = multiprocessing.Pool(4)
    result = pool.map_async(resume_page, resume_link_list)
    result = [_ for _ in result.get() if _ != 0]
    if len(result) > 0:
        pool.close()
        pool.join()
        #=== TODO Insert db ===#
        try:
            col_bkk_resume_info.insert_many(result, ordered=False)
        except Exception as e:
            print("Error insert db", e)
    else:
        pool.terminate()
        pool.join()
    print("===== TASK DONE =====")


def resume_page(url):
    resume_id = int(re.search(r'\/\d+\/', url).group(0).replace('/', ''))
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
            r'[\s+]', '|', info_sum.text.strip().replace(" ", "")).split("|")
        info_sum_list = [_ for _ in info_sum_list if _ != ""]

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
        resume_want = info_main.select_one(
            "div#resume_want").select("div.span11.offset1")
        if len(resume_want) > 0:
            for row in resume_want:
                # pair {occupation, position}
                op_dict = {}
                for col in row.select('.span6'):
                    key = re.sub(r'[\d.\s+]', '', col.b.text)
                    val = re.sub(r'[\s+]', '', col.span.text)
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
            val = re.sub(r'[\s+]', '|', val.text.strip().replace(" ", ""))
            val_list = [_ for _ in val.split("|") if _ != ""]
            resume_edu.append(
                (key, val_list[1:3] + val_list[4:])
            )
    except Exception as e:
        pass
        # print("Step resume_edu", e)

    #=== Extract Experience ===#
    try:
        resume_exp = info_main.select("div.row-fluid.jsXp_row.padB5")
        if resume_exp != None:
            resume_exp_list = []
            for exp in resume_exp:
                work_info_list = [re.sub(r'[\s+]', ' ', _.text.strip()) for _ in exp.select_one(
                    "div.o.col000.span6.padV10H20.cor4.bg_lightyellow").select("span.padL10")]
                work_detail = exp.select_one("div.padB10.bb-code").text.strip()
                resume_exp_list.append((work_info_list, work_detail))
    except Exception as e:
        pass
        # print("Step resume_exp", e)

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

        resume_skill = info_main.select_one("div#resume_skill")
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
                            r'[\s+]', '', _.text).split(':')[1]] for _ in row.select('.pull-left')]
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
                            r'[\s+]', '', skill_soup.text).split(':')
                    except:
                        # skill_other too many values
                        continue
                    key = resume_skill_mapper[key]
                    if key == 'skill_typing':
                        val_list = re.findall(r'\d+', val)
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
        pass
        # print("Step resume_skill", e)

    #=== Extract Main Info ===#
    try:
        # Handle cast String into Integer
        try:
            age = int(info_sum_list[1])
        except:
            age = ''
        try:
            exprience = int(info_sum_list[2])
        except:
            exprience = 0
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
            'exprience': exprience,
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
        pass
        # print("Step exp_csv", e)

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
        pass
        # print("Step edu_csv", e)

    #=== Extract training ====#
    try:
        training_list = info_main.select_one(
            "#resume_skill + div").select("div.row-fluid")
        if training_list != None:
            for train in training_list:
                result_dict = {}
                tl = [re.sub(r'[\s+]', ' ', _.text.strip())
                      for _ in train.select("span")]
                for tm, td in zip(train_mapper, tl):
                    result_dict[tm] = td
                if len(result_dict) == 0:
                    break
                else:
                    resume_csv_dict['training_hist'].append(result_dict)
    except Exception as e:
        pass
        # print("Step training_csv", e)
    return resume_csv_dict


def checks_update():
    try:
        cursor = list(col_bkk_update.find(
            {'_id': 'dump_resume_info'}).limit(1))[0]
        return cursor['next_page']
    except:
        return 1


if __name__ == '__main__':
    start = datetime.datetime.now()
    print("Start time: {}".format(start))

    #=== Initial url ===#
    update_page = checks_update()
    print("===== Start extract jobbkk@: {} =====".format(update_page))
    init_resume_url = "https://www.jobbkk.com/resumes/lists/"

    #=== TODO Insert ===#
    resume_indexer(init_resume_url, update_page)

    #=== TODO Update ===#
    # Function update resume exists

    end = datetime.datetime.now()
    col_bkk_update.update_one({'_id': 'dump_resume_info'}, {
                              '$set': {'date_cal': end}}, upsert=True)
    print("End time: {}".format(end))
    print("Cost time: {}".format(end - start))
