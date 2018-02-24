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

#=== For Job Analysis ===#
pp = pprint.PrettyPrinter(indent=4)
header = {'User-Agent': 'Chrome/64.0.3282.186'}
base_url = "https://www.jobbkk.com"

#=== page_list ===#
resume_list = "/resumes/lists/"
job_list = "/jobs/lists/"

#=== page_detail ===#
resume_detail = "/resumes/detail/"
job_detail = "/resumes/detail/"

#=== Initial const ====#
occupation_dict = config.occupation_dict


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
    """
    while True:
        try:
            res = requests.get(url, headers=header, timeout=10)
            time.sleep(0.25)
            if res.status_code == 200:
                break
        except Exception as e:
            print("===== RETRY =====")
            print(e)
            print("Continue ...")
            continue
    return res
    """


def resume_indexer(intial_url, update_page):
    #=== Initial page ===#
    res = retry(intial_url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        total_page = int(soup.select_one(
            "div#total_resume_row span.colRed").text.replace(',', ''))
    except TypeError:
        print("No total_page")
    resume_page_list(intial_url)
    print("========== Extract Resume: {} items ==========".format(total_page))
    #=== Looping resume page ===#
    for page in range(update_page + 1, total_page // 15):
        url = base_url + resume_list + str(page)
        resume_page_list(url)


def job_indexer(intial_url, update_page):
    #=== Initial page ===#
    res = retry(intial_url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        total_page = int(soup.select_one(
            "div.row-fluid.tdF span.colRed").text.replace(',', ''))
    except TypeError:
        print("No total_page")
    job_page_list(intial_url)
    print("========== Extract Job: {} items ==========".format(total_page))
    #=== Looping resume page ===#
    for page in range(update_page + 1, total_page // 25):
        url = base_url + job_list + str(page)
        job_page_list(url)


def resume_page_list(url):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        resume_list = soup.select_one("div.pad5").select(
            "div.span5 a.checkClicklist")
    except TypeError:
        print("resume_page_list")
        pass
    #=== Looping extract resume ===#
    resume_url_list = [_['href'] for _ in resume_list]
    #=== Parallel [multiprocessing] ===#
    print("===== TODO Parallel =====")
    pool = multiprocessing.Pool(4)
    result = pool.map_async(resume_page, resume_url_list, chunksize=4)
    print("===== TASK DONE: {} =====".format(len(result.get())))
    pool.close()
    pool.join()


def job_page_list(url):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        job_list = soup.select_one("div.pad5").select("div.jsearchCon h6 a")
    except TypeError:
        print("job_page_list")
        pass
    #=== Looping extract job ===#
    job_url_list = [_['href'] for _ in job_list]
    #=== Parallel [multiprocessing] ===#
    print("===== TODO Parallel =====")
    pool = multiprocessing.Pool(4)
    pool.map_async(job_page, job_url_list, chunksize=4)
    pool.close()
    pool.join()


def job_page(url):
    url_job = "https://www.jobbkk.com/jobs/detail/162303/767335/%E0%B8%AE%E0%B8%B2%E0%B8%A5%E0%B9%8C%E0%B8%9F%20%E0%B8%AD%E0%B8%B0%20%E0%B8%94%E0%B8%B5%E0%B8%81%E0%B8%A3%E0%B8%B5%20%E0%B8%88%E0%B8%B3%E0%B8%81%E0%B8%B1%E0%B8%94/%E0%B8%AB%E0%B8%B2%E0%B8%87%E0%B8%B2%E0%B8%99,%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%82%E0%B8%B2%E0%B8%A2-%E0%B8%AA%E0%B9%88%E0%B8%87%E0%B9%80%E0%B8%AA%E0%B8%A3%E0%B8%B4%E0%B8%A1%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%82%E0%B8%B2%E0%B8%A2,%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B8%88%E0%B8%B1%E0%B8%94%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%A3%E0%B9%89%E0%B8%B2%E0%B8%99%20%E0%B8%AA%E0%B8%B2%E0%B8%82%E0%B8%B2%E0%B9%80%E0%B8%8A%E0%B8%B5%E0%B8%A2%E0%B8%87%E0%B9%83%E0%B8%AB%E0%B8%A1%E0%B9%88%20%E0%B8%97%E0%B9%88%E0%B8%B2%E0%B9%81%E0%B8%9E"
    res = retry(url_job)
    soup = BeautifulSoup(res.content, 'lxml')

    #=== Static JobBKK ===#
    try:
        static_detail_list = [_.text.strip()
                              for _ in soup.select("div.statis-detail")]
    except Exception as e:
        print("Bug job_static", e)

    #=== Interesting ===#
    try:
        applicants = soup.select_one("#loadnumapply").text.strip()
    except Exception as e:
        print("Bug job_interesting", e)

    #=== Social Media ===#
    try:
        social = soup.select_one(".company-social").select('a')
        social = [_.text.strip() for _ in social if _.text.strip() != "-"]
        email = social.pop()
        cnt_social = len(social)
    except Exception as e:
        print("Bug job_social_media", e)

    #=== Info ===#
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
        print("Bug job_info", e)

    #=== Transport ===#
    try:
        jobLocation = info.select_one("div[itemprop=jobLocation]")
        transport_detail_list = [_.text.strip().replace(
            'ไม่มี', '') for _ in jobLocation.select("div.transport-detail")]
        # transport_additional = jobLocation.select_one("div.transport-additional span").text.strip()
    except Exception as e:
        print("Bug job_transport", e)

    #=== Json Data ===#
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
        print("Bug job_json_data", e)

    #=== Location Company ===#
    # street_address = data_dict['jobLocation']['address']['streetAddress']
    # local_address = data_dict['jobLocation']['address']['addressLocality']
    # region_address = data_dict['jobLocation']['address']['addressRegion']
    # postal_code = data_dict['jobLocation']['address']['postalCode']
    # country_address = data_dict['jobLocation']['address']['addressCountry']

    # Result Dict
    if re.search('-', skill_list[2]) != None:
        edu_clean = skill_list[2].replace(' ', '').split('-')
        edu_clean = config.edu_dict[edu_clean[0]] + \
            '-' + config.edu_dict[edu_clean[1]]
    else:
        try:
            edu_clean = config.edu_dict[edu_clean]
        except KeyError:
            print("Bug KeyError: ", edu_clean)
            edu_clean = ""
    try:
        job_dict = OrderedDict({
            'job_id': int(job_com_id[1]),
            'job_title': job_title,
            'job_description': description.replace('\n', '|'),
            'num_position': int(detail_list[0].replace('ตำแหน่ง', '').replace('ไม่ระบุ', '').replace('ไม่จำกัด', 'Inf').strip()),
            'job_type': detail_list[1],
            'company_id': int(job_com_id[0]),
            'company': company,
            'work_location': detail_list[2].replace(',', '|'),
            'salary': detail_list[3].replace(',', '').replace(' ', ''),
            'vacation': detail_list[5].replace('ไม่ระบุ', ''),
            'work_time': detail_list[4].replace('ไม่ระบุ', ''),
            'gender': skill_list[0].replace(' ', '').replace('ชาย', 'M').replace('หญิง', 'F').replace(',', ''),
            'age': skill_list[1].replace('ปีขึ้นไป', '+').replace('ทุกช่วงอายุ', '').replace(' ', ''),
            'edu': edu_clean.strip(),
            'exp': skill_list[3].replace('ปีขึ้นไป', '+').replace(' ', ''),
            'other': skill_list[4].replace('ไม่ระบุ', ''),
            'incentives': '|'.join(incentives_detail_list),
            'incentives_add': incentives_additional.replace(' ', '|'),
            'email': email,
            'social_pnt': cnt_social,
            'trans_bus': transport_detail_list[0],
            'trans_bts': transport_detail_list[1],
            'trans_mrt': transport_detail_list[2],
            'trans_arl': transport_detail_list[3],
            'applicants': int(applicants),
            'job_active': static_detail_list[1],
            'job_view': int(static_detail_list[0].replace(',', '')),
            'job_date_post': date_post,
        })
    except Exception as e:
        print("Bug job_dict", e)
    try:
        col_bkk_job.insert(job_dict)
    except Exception as e:
        print('db', e)


def resume_page(url):
    res = retry(url)
    soup = BeautifulSoup(res.content, 'lxml')
    try:
        #=== Extract Info Summary ===#
        info_sum = soup.select_one(
            "div#box_right div.span11.marL10.padB10.padL20")
        prog = info_sum.select_one("div#progress_resume div")[
            'style'].strip()[-3:].replace('%', '')
        [div.decompose() for div in info_sum.select("div")]
        [b.decompose() for b in info_sum.select("b")]
        info_sum_list = re.sub(
            '[\s+]', '|', info_sum.text.strip().replace(" ", "")).split("|")
        info_sum_list = list(filter(lambda _: _ != "", info_sum_list))

        #=== Extract Info Resume ===#
        info_main = soup.select_one("div#resumeDT")
        try:
            resume_id = re.search('\/\d+\/', url).group(0)
        except:
            return 0
        resume_update = info_main.select_one(
            "div.taR.marR10").text.strip().split(":")

        """
        TODO DEBUG resument_want
        """
        resume_want = [_.text.strip().replace(" ", "").replace("\n", "").replace(
            "\r", "") for _ in info_main.select_one("div#resume_want").select("span.padL10.inline")]
    except Exception as e:
        print("Step Info_resume", e)

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
        pass
        # print("Step resume_edu", e)

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
        pass
        # print("Step resume_exp", e)

    #=== Extract Skill[No Stable] ===#
    try:
        resume_skill = info_main.select_one("div#resume_skill")
        resume_skill_list = []
        lang_list = []
        if resume_skill != None:
            for skill_soup in resume_skill.select("div.padV10H20 > div.span11.offset1"):
                try:
                    skill_soup['style']
                except KeyError:
                    skill_soup['style'] = None
                if skill_soup['style'] == "float:left":
                    for _ in skill_soup.select("div.span2.bg_lightyellow.taCen.o"):
                        lang_list.append(_.text.strip())
                    skill_lang = skill_soup.select("div.span10")
                    for _ in skill_lang:
                        resume_skill_list.append(
                            _.text.strip().replace(" ", "").replace("\n", "|"))
                else:
                    try:
                        skill_soup.select_one("b").decompose()
                    except AttributeError:
                        pass
                    skill = re.sub(
                        '[\s+]', ' ', skill_soup.text.strip().replace(" ", ""))
                    resume_skill_list.append(skill)

            """
            TODO DEBUG
            """
            resume_skill_list[3] = resume_skill_list[3].replace(" ", "|")
            resume_skill_list[-1] = resume_skill_list[-1].replace(
                "Computer:", "")
            skill_typing_th, skill_typing_en = resume_skill_list[3].split("|")
            skill_lang = resume_skill_list[4:]
    except Exception as e:
        print("Step resume_skill", e)

    #=== Start Main Info ===#
    try:
        resume_csv_dict = OrderedDict({
            'resume_id': int(resume_id.replace('/', '')),
            'resume_modified': resume_update[1].strip(),
            'resume_progress': int(prog),
            'gender': info_sum_list[0].replace('ชาย', 'M').replace('หญิง', 'F'),
            'age': int(info_sum_list[1]),
            'exprience': int(info_sum_list[2]),
            'job_type': resume_want[-4],
            'asked_job': [],
            'asked_location': resume_want[-5],
            'asked_salary': resume_want[-3],
            'start_in': resume_want[-2],
            'work_aboard': resume_want[-1].replace('ได้', 'yes').replace('ไม่ได้', 'no'),
            'edu_hist': [],
            'exp_hist': [],
            'own_vehicle': None,
            'drive_license': None,
            'skill_lang': [],
            'skill_typing_th_wm': None,
            'skill_typing_en_wm': None,
            'training_hist': []
        })
    except Exception as e:
        print("Step resume_csv", e)

    """
    TODO DEBUG
    """
    #=== Add asked_job ===#
    try:
        job_cp = resume_want[:-5]
        for _ in range(0, len(job_cp), 2):
            resume_csv_dict['asked_job'] .append({'occupation': job_cp[_], 'position':job_cp[_ + 1]})
    except Exception as e:
        print("Step job_cp_csv", e)
    #=== End Main Info ===#

    #=== Start Additional Info ===#
    if len(resume_skill_list) != 0:
        try:
            #=== Fill Missing Value ===#
            resume_csv_dict['own_vehicle'] = resume_skill_list[1].split(','),
            resume_csv_dict['drive_license'] = resume_skill_list[2].split(','),
            resume_csv_dict['skill_typing_th_wm'] = skill_typing_th
            resume_csv_dict['skill_typing_en_wm'] = skill_typing_en
            # resume_csv_dict['skill_other'] = None
            # resume_csv_dict['skill_other'] = resume_skill_list[6].replace(
            #     ",", "|")
        except Exception as e:
            print('Step skill_typing csv', e)

        #=== Add skill_lang ===#
        if skill_lang != None:
            try:
                for lang_name, skill in zip(lang_list, skill_lang):
                    result_dict = {}
                    for sk, data in zip(config.skill_mapper, skill.split("|")):
                        result_dict[sk] = config.lang_mapper[data.split(":")[1]]
                    if len(result_dict) == 0:
                        break
                    else: 
                        resume_csv_dict['skill_lang'].append({'name': lang_name, 'skill': result_dict})
            except Exception as e:
                print("Step skill_lang", e)

    #=== Add exp ===#
    try:
        if len(resume_exp_list) != 0:
            for rel in resume_exp_list:
                result_dict = {}
                for exp, _ in zip(config.exp_mapper, rel[0]):
                    result_dict[exp] = _
                resume_csv_dict['exp_hist'].append({'exp_info': result_dict, 'exp_detail': rel[1]})
    except Exception as e:
        pass
        # print("Step exp_csv", e)
    
    #=== Add education ===#
    try:
        if len(resume_edu) != 0:
            for redu in resume_edu:
                result_dict = {'edu_year': redu[0].replace(
                    'กำลังศึกษาอยู่', 'Studying')}
                for edu, r in zip(config.edu_mapper, redu[1]):
                    if edu == "edu_level":
                        try:
                            r = config.edu_dict[r]
                        except KeyError:
                            # print("KeyError: ", r)
                            r = config.edu_dict[r.replace(
                                '(กำลังศึกษาอยู่)', '')]
                            r = r + ' (Studying)'
                    result_dict[edu] = r
                resume_csv_dict['edu_hist'].append(result_dict)
    except Exception as e:
        pass
        # print("Step edu_csv", e)

    #=== Add training ====#
    try:
        training_list = info_main.select_one(
            "#resume_skill + div").select("div.row-fluid")
        if training_list != None:
            for train in training_list:
                result_dict = {}
                tl = [re.sub('[\s+]', ' ', _.text.strip())
                      for _ in train.select("span")]
                for tm, td in zip(config.train_mapper, tl):
                    result_dict[tm] = td
                if len(result_dict) == 0:
                    break
                else: 
                    resume_csv_dict['training_hist'].append(result_dict)     
    except Exception as e:
        pass
        # print("Step training_csv", e)
    #=== End Additional Info ===#

    #=== TODO Nosql ===#
    try:
        col_bkk_resume.insert_one(resume_csv_dict)
    except Exception as e:
        print("db", e)
    return 1


if __name__ == '__main__':
    start = datetime.datetime.now()
    print("Start time: {}".format(start))

    #=== Initial url ===#
    update_page = 10
    start_resume_url = "https://www.jobbkk.com/resumes/lists/" + str(update_page)
    start_job_url = "https://www.jobbkk.com/jobs/lists/" + str(update_page)

    #=== TODO Insert ===#
    resume_indexer(start_resume_url, update_page)
    # job_indexer(start_job_url, update_page)
    #=== TODO Update ===#

    end = datetime.datetime.now()
    print("End time: {}".format(end))
    print("Cost time: {}".format(end - start))
