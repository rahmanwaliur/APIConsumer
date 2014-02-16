#import requests
import json

import genhive
import submitjob

# response = requests.get('https://github.com/timeline.json')

with open("data.json") as filename:
  parsed_data = json.loads(filename.read())

rownum = 0

for row in parsed_data:
    parsed_job = parsed_data[rownum]
    jobparams =  parsed_job["medcodes"]

    hivequery = genhive.hivequery(jobparams)
    submitjob.newjob(hivequery)

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