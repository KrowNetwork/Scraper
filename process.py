import json 
import requests 
from bs4 import BeautifulSoup
import html2text
import datetime
import rfc3339
import os
import uuid
from googleapiclient.discovery import build
from googleapiclient.errors import Error
import time

credential_path = "TUCKER-krow-network-1533419444055-32d5a289781e.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
os.environ['GOOGLE_CLOUD_PROJECT'] = "krow-network-1533419444055"

parent = 'projects/' + os.environ['GOOGLE_CLOUD_PROJECT']
client_service = build('jobs', 'v3')
bad_sources = ["Neuvoo"]

# with open('data.json') as f:
#     data = json.load(f)
def get_education_level(text):
    degree_types = ["DEGREE_TYPE_UNSPECIFIED"]

    # if "bachelor" in text.lower() or " bs" in text.lower():
    #     if degree_types[0] == "DEGREE_TYPE_UNSPECIFIED":
    #         degree_types = []
    #     degree_types.append("BACHELORS_OR_EQUIVALENT")
    # if "master's" in text.lower() or "masters" in text.lower() or " ms" in text.lower():
    #     if degree_types[0] == "DEGREE_TYPE_UNSPECIFIED":
    #         degree_types = []
    #     degree_types.append("MASTERS_OR_EQUIVALENT")

    return degree_types

def create_company(client_service, company_to_be_created):
    try:
        request = {'company': company_to_be_created}
        company_created = client_service.projects().companies().create(
            parent=parent, body=request).execute()
        print('Company created: %s' % company_created)
        return company_created
    except Error as e:
        return str(e)

def get_all_jobs(client_service, company_name):
    try:
        company_name = 'companyName="' + company_name + '"'
        company_existed = client_service.projects().jobs().list(
            parent=parent, filter=company_name).execute()
        # print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting jobs')
        # time.sleep(10)
        raise e

def get_company(client_service, company_name):
    try:
        company_existed = client_service.projects().companies().get(
            name=company_name).execute()
        print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting company')
        raise e

def find_company(client_service, externalId):
    response, request = get_all_companies(client_service, save=True)
    while True:
        for i in response["companies"]:
            if i["externalId"] == externalId:
                return i

        response, request = get_next_companies(client_service, response, request)
    

def get_next_companies(client_service, previous_response, previous_request):
    try:
        company_existed = client_service.projects().companies().list_next(
            previous_request=previous_request, previous_response=previous_response)
        ret = company_existed.execute()
        # print('Company existed: %s' % company_existed)
        return ret, company_existed
    except Error as e:
        print('Got exception while getting company')
        raise e

def get_all_companies(client_service, pageToken=None, save=False):
    try:
        company_existed = client_service.projects().companies().list(
            parent=parent, pageToken=pageToken)
        ret = company_existed.execute()
        # print('Company existed: %s' % company_existed)
        if save:
            return ret, company_existed
        else:
            return ret
    except Error as e:
        print('Got exception while getting company')
        raise e

def create_job(client_service, job_to_be_created):
    try:
        request = {'job': job_to_be_created}
        job_created = client_service.projects().jobs().create(
            parent=parent, body=request).execute()
        # print('Job created: %s' % job_created["title"])
        return job_created
    except Error as e:
        raise e
        # print('Got exception while creating job')
        # raise e

def batch_create(jobs):
    # client_service = build('jobs', 'v3')
    created = 0
    errors = ""
    # print (jobs)
    for i in jobs:
        try:
            create_job(client_service, i)
            created += 1
        except Exception as e:
            errors += str(e) + "\n"
            # print (i["requisition_id"])
    print (errors)
    print ("Created %s jobs" % created)
    # created = 0
    # def job_create_callback(request_id, response, exception):
    #     if exception is not None:
    #         # print (response)
    #         print('Got exception while creating job: %s' % exception)
    #         pass
    #     else:
    #         # print('Job created: %s' % response)
    #         # created_jobs.append(response)
    #         # created += 1
    #         pass

    # try:
    #     batch = client_service.new_batch_http_request()

    #     for i in jobs:
    #         # print (i)
    #         try:                
    #             # print (i["requisitionId"])
    #             x = client_service.projects().jobs().create(parent=parent, body={"job": i})
    #             batch.add(x, callback=job_create_callback)
    #         except Exception as e:
    #             # print (e)
    #             print (i["requisition_id"])
    #     # try:   
    #     batch.execute()
    #     if len(jobs) != 0:
    #         print ("%s Jobs Created" % len(jobs))
    # except Error as e:
    #     print (e)
    #     # print('Got exception while creating job')
    #     raise e


def generate_job(**kwargs):

    custom_attributes = {
        'main_source': {
            'string_values': [kwargs["source"]],
            'filterable': True
    },
        'api_src': {
            'string_values': ["ZipRecruiter"],
            'filterable': True
    }}
    #     'url_list': {
    #         'string_values': [kwargs["url"]],
    #         'filterable': True
    # }}

    job = {
        "company_name": kwargs["name"],
        "requisition_id": kwargs["id"],
        "title": kwargs["title"],
        "description": kwargs["text"],
        "application_info": {"uris": kwargs["url"]},
        "custom_attributes": custom_attributes,
        "postingExpireTime": kwargs["expiration"],
        "postingPublishTime": kwargs["date"],
        "addresses": [kwargs["location"]],
        "degreeTypes": kwargs["degree_types"]

    }   

    # print (job["company_name"])
    # exit()
    return job
    # try:
    #     create_job(client_service, job)
    # except:
    #     # print ("Job already created!")
    #     pass

def get_company_name(i):
    # print (i)
    comp = i["hiring_company"]["name"]

    try: 
        name = create_company(client_service, {'display_name': comp, 'external_id': "_".join(comp.split())})
        if name.startswith("<"):
            # print (name)
            # exit()
            name = name.split('"')[1].split(" ")[1]
        else:
            name = name["name"]
        # print (name)
        return name
    except Exception as e:
        # raise e
        return None
        # x = get_all_companies(client_service)
        # for a in x["companies"]:
        #     # print (i)
        #     # i = json.loads(i)
        #     if a["externalId"] == "_".join(comp.split()):
        #         print (a)
        #         name = a["name"]
        #         return name
        #         break
    # print (name)
        

# def get_page(url, src_lower):
   
#     print (page)
#     print (page.url)
#     print (page.content)
#     if str(page.status_code) == str(302) or src_lower not in str(page.content):
#         while True:
#             page = requests.get(page.url, allow_redirects=True)
#             print (page.url)
#             print (src_lower in page.content)

def process(i):
    # print (i["source"])
    # if i["source"] not in bad_sources:
    dice_split = False
    class_ = False
    if i["source"].lower() == "ziprecruiter":
        src = "ZipRecruiter"
        id_ = "job_desc"
    elif "dice" == i["source"].lower():
        src = "Dice"
        id_ = "jobdescSec"
    elif "dice.com" == i["source"].lower():
        src = "Dice.com"
        id_ = "jobdescSec"
        dice_split = True
        # print ("dice")
    elif "monster" in i["source"].lower():
        src = "Monster"
        id_ = "JobDescription"
    elif "clearancejobs.com" in i["source"].lower():
        src = "ClearanceJobs.com"
        id_ ={"itemprop": "description"}
    elif "glassdoor" in i["source"].lower():
        src = "Glassdoor"
        id_ = "jobDesc"
        class_ = True
    elif "linkedin" in i["source"].lower():
        src = "LinkedIn"
        id_ = "description"
        class_ = True
    elif "efinancialcareers" in i["source"].lower():
        src = "eFinancialCareers"
        id_ = "body"
        class_ = True
    try:
        # print (i)
        name = get_company_name(i)
        # print ("n %s" % name)
        if name != None:

            url = i["url"]
            if dice_split == True:
                url2 = "https://www.dice.com/jobs/detail/" + url.split("_")[1]
            else:
                url2 = url

            # print (url2)
  
            # requisition_id = str(uuid.uuid4())
            
                    # elif j[""]
            requisition_id = "_".join(i["hiring_company"]["name"][:75].split()) + "_" + "_".join(i["name"].split())[:180]# + "_" + str(uuid.uuid4())[:15]
            # print (requisition_id)
            # print (len(requisiton_id))
            # exit()
            # print (requisition_id)
            # print (type(requisition_id))
            # print (len(requisition_id))
            # exit()
            # print (url)
            page = requests.get(url2, allow_redirects=True)
            # print (page.url)
            # print (page.content)        
            soup = BeautifulSoup(page.content, 'html.parser')
            if type({}) == type(id_):
                x = soup.find(attrs=id_).decode_contents()
            elif class_ and src == "LinkedIn":
                x = soup.find(attrs={"class": id_}).div.decode_contents()
            elif class_ and src == "eFinancialCareers":
                x = soup.find(attrs={"class": id_}).decode_contents()
            else:
                x = soup.find(id=id_).decode_contents()
            # print (x)
            # exit()
            text = html2text.html2text(x)

            jobs = get_all_jobs(client_service, name)
            if "jobs" in jobs:
                for j in jobs["jobs"]:
                    if j["name"] == i["name"]:
                        print ("Dup")
                        raise Exception 
                    if j["description"] == text:
                        print ("Dup")
                        raise Exception

            date = datetime.datetime.strptime(i["posted_time"], "%Y-%m-%dT%H:%M:%S")
            expiration = date + datetime.timedelta(days=30)

            date = rfc3339.rfc3339(date)
            expiration = rfc3339.rfc3339(expiration)

            # print (expiration.isoformat("T") + "Z")
            # exit()

            degree_types = get_education_level(text)

            return generate_job(
                name=name,
                id=requisition_id,
                title=i["name"],
                text=text,
                url=i["url"],
                expiration=str(expiration),
                date=str(date),
                location=i["location"],
                source=i["source"],
                src=src,
                degree_types=degree_types
            )
        else:
            return None
        
    except Exception as e:
        # raise(e)
        if str(e).startswith("<HttpError 429") == False:
            print (id_)
            print (i["source"])
            print (i["url"])
            print (src)
            print (e)
        else:
            print ("Sleeping for 90 seconds due to HTTP error code 429: Quota exceeded")
            time.sleep(90)
        # raise(e)
        return None
        




# def batch_job_create(client_service, company_name):
#     import base_job_sample
#     created_jobs = []

#     def job_create_callback(request_id, response, exception):
#         if exception is not None:
#             print('Got exception while creating job: %s' % exception)
#             pass
#         else:
#             print('Job created: %s' % response)
#             created_jobs.append(response)
#             pass

#     batch = client_service.new_batch_http_request()
#     job_to_be_created1 = base_job_sample.generate_job_with_required_fields(
#         company_name)
#     request1 = {'job': job_to_be_created1}
#     batch.add(
#         client_service.projects().jobs().create(parent=parent, body=request1),
#         callback=job_create_callback)

#     job_to_be_created2 = base_job_sample.generate_job_with_required_fields(
#         company_name)
#     request2 = {'job': job_to_be_created2}
#     batch.add(
#         client_service.projects().jobs().create(parent=parent, body=request2),
#         callback=job_create_callback)
#     batch.execute()

#     return created_jobs
#         # exit()






