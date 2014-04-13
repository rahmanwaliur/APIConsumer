import requests
import json

import genhive
import submitjob
import subprocess
import thread
import time
# response = requests.get('https://github.com/timeline.json')

with open("data.json") as filename:
  parsed_data = json.loads(filename.read())
  length = len(parsed_data)
  #print parsed_data

firsttime = 0
rownum = 0
flag = 1
i = 80
j = 0
for row in parsed_data:
    if (length == rownum+1):
        flag = 0
    parsed_job = parsed_data[rownum]
    jobparams =  parsed_job["medcodes"]
    cohort = parsed_job["cohortname"]
    user = parsed_job["userid"]
    hivequery = genhive.hivequery(user, cohort, jobparams)

    thread.start_new_thread(submitjob.newjob,(hivequery,flag))
    time.sleep(1)
    #alljobs = subprocess.check_output('/usr/local/hadoop/bin/hadoop job -list', shell=True)
    proc = subprocess.Popen(["/usr/lib/hadoop/bin/hadoop", "job", "-list"], stdout=subprocess.PIPE)
    #proc = subprocess.Popen(["/usr/local/hadoop/bin/hadoop", "job", "-list"], stdout=subprocess.PIPE)
    alljobs, err = proc.communicate()
    responsestring = {"id": parsed_job["jobid"], "jobId": alljobs[i:i+21]}
    #print responsestring
    print alljobs
    ##url = 'http://localhost:8000/job/create/?'

    ##for key, value in responsestring.iteritems():
    ##    url += '%s=%s&' % (key, value)
    ##url = url[:-1]
    ##requests.get(url)
    i = i+55
    rownum = rownum + 1





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
ifile.close()"""