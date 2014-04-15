import requests
import json

import genhive
import submitjob
import subprocess
import thread
import string
import time
# response = requests.get('https://github.com/timeline.json')

with open("data.json") as filename:
  parsed_data = json.loads(filename.read())
  length = len(parsed_data)
  #print parsed_data



rownum = 0
flag = 1

for row in parsed_data:
    if (length == rownum+1):
        flag = 0
    parsed_job = parsed_data[rownum]
    jobparams =  parsed_job["medcodes"]
    cohort = parsed_job["cohortname"]
    user = parsed_job["userid"]
    hivequery = genhive.hivequery(user, cohort, jobparams)

    thread.start_new_thread(submitjob.newjob,(hivequery,flag))

    time.sleep(7)
    #alljobs = subprocess.check_output('/usr/local/hadoop/bin/hadoop job -list', shell=True)
    proc = subprocess.Popen(["/usr/lib/hadoop/bin/hadoop", "job", "-list"], stdout=subprocess.PIPE)
    #proc = subprocess.Popen(["/usr/local/hadoop/bin/hadoop", "job", "-list"], stdout=subprocess.PIPE)
    alljobs, err = proc.communicate()
    linecount = alljobs.count('\n')
    lines = alljobs.splitlines()
    jobline = lines[linecount-1]

    jobIDIndex = jobline.find("job_")
    jobID = jobline[jobIDIndex:jobIDIndex+21]
    responsestring = {"id": parsed_job["jobid"], "jobId": jobID}
    print responsestring
    rownum = rownum + 1


    #print alljobs
    ##url = 'http://localhost:8000/job/create/?'

    ##for key, value in responsestring.iteritems():
    ##    url += '%s=%s&' % (key, value)
    ##url = url[:-1]
    ##requests.get(url)




#### In Case we use CSV file as API Response

"""import csv

ifile = open('data.csv', "rb")
reader = csv.reader(ifile)
rownum = 0
for row in reader:
#save header row
  if rownum == 0:
    header = row
  else:
    colnum = 0
    for col in row:
      if header[colnum] == "medcodes":
        #print '%s' % (col)
        job = genhive.hivequery(col)
	#print job
        # HiveJob(codes).run()
	submitjob.newjob(job)
      colnum = colnum + 1
  rownum = rownum + 1
ifile.close()

1 jobs currently running
JobId	State	StartTime	UserName	Priority	SchedulingInfo
job_201402030754_0223	4	1397500639898	wrahman	NORMAL	NA

2 jobs currently running
JobId	State	StartTime	UserName	Priority	SchedulingInfo
job_201402030754_0223	1	1397500639898	wrahman	NORMAL	6 running map tasks using 6 map slots. 0 additional slots reserved. 0 running reduce tasks using 0 reduce slots. 0 additional slots reserved.
job_201402030754_0224	4	1397500648629	wrahman	NORMAL	NA
"""