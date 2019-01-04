import json 
import requests 
from bs4 import BeautifulSoup
import html2text
import datetime
import rfc3339
import os
import time
from googleapiclient.discovery import build
from googleapiclient.errors import Error
import csv



credential_path = "TUCKER-krow-network-1533419444055-32d5a289781e.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
os.environ['GOOGLE_CLOUD_PROJECT'] = "krow-network-1533419444055"

parent = 'projects/' + os.environ['GOOGLE_CLOUD_PROJECT']
client_service = build('jobs', 'v3')
# bad_sources = ["Neuvoo"]

def get_all_jobs(client_service, company_name):
    try:
        company_existed = client_service.projects().jobs().list(
            parent=parent, filter=company_name).execute()
        # print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting jobs')
        time.sleep(10)
        raise e

def get_all_companies(client_service, pageToken=None):
    try:
        company_existed = client_service.projects().companies().list(
            parent=parent, requireOpenJobs=True, pageToken=pageToken).execute()
        # print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting company')
        time.sleep(10)
        raise e

def delete_job(client_service, name):
    try:
        company_existed = client_service.projects().jobs().delete(
            name=name).execute()
        # print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting company')
        raise e

def batch_delete(client_service, names):
    try:
        batch = client_service.new_batch_http_request()
        # print (len(names))
        for name in names:
            # print (i)
            try:
                x = client_service.projects().jobs().delete(name=name["name"])
                batch.add(x)
            except Exception as e:
                print (e)
            
        batch.execute()
        print ("%s Jobs Deleted" % len(names))
    except Error as e:
        print (e)
        # print('Got exception while creating job')
        raise e

count = 0
comps_data = get_all_companies(client_service)
# print(comps_data)
# comps_data["companies"] = [{"name": "projects/krow-network-1533419444055/companies/70f2478b-53a2-4c41-906c-8214d217fddc"},
#                             {"name": "projects/krow-network-1533419444055/companies/ffbcf50a-3729-49f7-bb5d-7d8c0246e470"},
#                             {"name": "projects/krow-network-1533419444055/companies/fe4fa141-2ac1-452b-bb59-fd7ecfbee358"}]
# # # print (comps_data)
# comps = comps_data["companies"]
# print (comps_data)
# print (len(comps_data["companies"]))
u = 0
with open('data.json', 'w') as outfile:
    outfile.write("")
while True:
    try:
        if "companies" in comps_data:
            comps = comps_data["companies"]
            done = 0
            x = len(comps)
            for c in range(x):
                company = comps[0]
                jobs_data = get_all_jobs(client_service, 'companyName="' + company["name"] + '"')
                comps_data["companies"].pop(0)
                # if "jobs" in jobs_data:
                #     jobs.extend(jobs_data["jobs"])
                    # batch_delete(client_service, jobs_data["jobs"])
                    # for i in jobs_data["jobs"]:
                    #     jobs.append(i)
                    # if len(jobs) > 0:
                    #     while len(jobs) != 0:                
                    #         # j = jobs[:50]
                    #         # del jobs[:50]
                    #         batch_delete(client_service, jobs)
                    #         jobs = []
                    #     # jobs = []
                    # else:
                    #     # print ("Current array length: %s" % len(jobs))
                    #     pass

                # done += 1
                    # jobs.extend(jobs_data["jobs"])
                    


                    

                # else:
                #     pass
                # with open("out.csv","w",newline="") as f:
                    # cw = csv.writer(f,delimiter=",")
                    # cw.writerows(jobs_data["jobs"])
                with open('data.json', 'a') as outfile:
                    for i in jobs_data["jobs"]:
                        json.dump(i, outfile)
                        outfile.write("\n")
                        
                    u += len(jobs_data["jobs"])

            print (u)
        
        comps_data = get_all_companies(client_service, pageToken=comps_data["nextPageToken"])
    except:
        print ("Sleeping")
        time.sleep(10)
        # try:
        #     # comps_data = get_all_companies(client_service, pageToken=comps_data["nextPageToken"])
        # except:
        #     print ("big sleep")
        #     time.sleep(180)
            # comps_data = get_all_companies(client_service, pageToken=comps_data["nextPageToken"])
    # comps = comps_data["companies"]
    # print (comps_data)

    # if len(comps) == 0:
    #     break
            





# for i in :
#     names = []
#     print (i)
#     jobs = get_all_jobs(client_service, 'companyName="' + i["name"] + '"')
#     print (jobs)
    # try:
    #     for j in ["jobs"]:
    #         # batch_delete(client_service, )
    #         names.append(j["name"])
    #     z = 24
    #     p = 0
    #     print (names)
    #     while True:
    #         n = names[p:z]
    #         p += 24
    #         z += 24
    #         batch_delete(client_service, n)
    #         count += len(n)
    #         break
    #     print (count)

    # except:
    #     pass
        
        
    # print (i["name"])
    # print (i)
    # exit()
