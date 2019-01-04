import requests
import pandas as pd
import random 
import json
import process
import time

locations = pd.read_csv("locations.csv")
area = str(200)
cities = locations[["city"]].values[:40]
states = locations[['state']].values[:40]
locs = []
for c, s in zip(cities, states):
    c = c[0]
    s = s[0]
    st = c + "," + s 
    st = st.replace(" ", "%20")
    locs.append(st)

t = pd.read_csv("job-titles.csv")
titles_ = t[["Title"]].values
print (titles_)

titles = []
for t in titles_:
    t = t[0]
    t = t.replace(" ", "%20")
    titles.append(t)

# x = titles[:15]
# titles = titles[15:]
# random.shuffle(titles)
# x.extend(titles)
# titles = x
# print (titles)
# random.shuffle(titles)
random.shuffle(titles)

# titles = titles[:300]
# titles = titles[4:]

good_sources = ["ziprecruiter", "monster.com", "monster", "dice", "dice.com", "clearancejobs.com", "efinancialcareers"]

print ("getting jobs")
x = 0
c = 0
z = 0
jobs = []
j = []
# titles[0] = "Machine Learning"
for t in titles:
    z += 1
    print ("%s | %s" % (z, x))
    print ("Job: %s" % t.replace("%20", " "))
    # random.shuffle(locs)
    for l in locs:
        print ("Location: %s | %s" % (l.replace("%20", " "), len(j)))
        try:
            url = "https://api.ziprecruiter.com/jobs/v1?search=" + t + "&location=" + l + "&radius_miles=" + area + "&days_ago=7&jobs_per_page=1000&page=1&api_key=faaqxf98564y5xg422fra9ctyftgk3iw"
            # print (url)
            r = requests.get(url)

            jobs = r.json()["jobs"]
        except:
            break
        # exit()
        y = 1
        
        for i in jobs[:10]:
            # # print (i["source"])
            if i["source"].lower() in good_sources:
                g = process.process(i)
                if g != None:
                    j.append(g)
            #     print (i["source"])
            #     print (i["url"])
            # if i["source"].lower() == "efinancialcareers":
                # print (i)
                # print(g)
                # exit()
            # elif i["source"].lower() not in good_sources:
            #     print (i["source"])
            #     print (i["url"])
                #     # break
        # exit()
        if len(j) > 50:
            x += len(j)
            # j = [j[0]]
            # print (j)
            process.batch_create(j)
            # exit()

            # break
            j = []
            # break

                
        # print (j)
        # if len(j) > 25
        
        jobs = []
     
            

    if x > 295:
        print ("sleeping")
        # time.sleep(60)
        x = 0

print (x)