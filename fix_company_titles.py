import json 
import requests 
from bs4 import BeautifulSoup
import html2text
import datetime
import rfc3339
import os
from googleapiclient.discovery import build
from googleapiclient.errors import Error

credential_path = "TUCKER-krow-network-1533419444055-32d5a289781e.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
os.environ['GOOGLE_CLOUD_PROJECT'] = "krow-network-1533419444055"

parent = 'projects/' + os.environ['GOOGLE_CLOUD_PROJECT']
client_service = build('jobs', 'v3')
bad_sources = ["Neuvoo"]
def get_all_companies(client_service, pageToken=None):
    try:
        company_existed = client_service.projects().companies().list(
            parent=parent, pageSize=100, pageToken=pageToken).execute()
        # print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting company')
        raise e

def update_company(client_service, body, name):
    try:
        company_existed = client_service.projects().companies().patch(
            name=name, body=body).execute()
        # print('Company existed: %s' % company_existed)
        return company_existed
    except Error as e:
        print('Got exception while getting company')
        raise e

companies = []
x = get_all_companies(client_service)
companies.extend(x["companies"])
print (companies[0])
while True:
    x = get_all_companies(client_service, x["nextPageToken"])
    companies.extend(x["companies"])
    if len(x["companies"]) != 100:
        break
    print (len(x["companies"]))
print (len(companies))

c = 0
for i in companies:
    if i["displayName"].lower() != i["displayName"]:
        i["displayName"] = i["displayName"].lower()
        i["externalId"] = i["externalId"].lower()
        # print (i)
        # print (i["name"])
        update_company(client_service, {"company": i}, i["name"])
        c += 1
    if c % 50 == 0:
        print (c)
        

