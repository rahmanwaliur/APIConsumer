import pyhs2
import subprocess


def newjob(ajob, flagvalue):

  conn = pyhs2.connect(host='136.159.79.112', port = 10000, authMechanism = "PLAIN", user='wrahman', password='suman@ASE13', database='default')

  #conn = pyhs2.connect(host='localhost', port = 10000, authMechanism = "PLAIN", user='hduser', password='suman@ASE13', database='default')

  cur = conn.cursor()
  cur.execute(ajob['createdb'])
  cur.execute(ajob['workdb'])
  cur.execute(ajob['droptable1'])
  cur.execute(ajob['createtable1'])
  cur.execute(ajob['testcode'])
  cur.execute(ajob['droptable2'])
  cur.execute(ajob['createtable2'])

          #for i in cur.fetch()
          #    print i

  #job_id = subprocess.check_output('/usr/local/hadoop/bin/hadoop job -list')
  #print job_id

  if flagvalue == 0:
      cur.close()
      conn.close()


"""class HiveJob(threading.Thread):

  def run(self):
     # add job entry to sqlite database as incomplete w/ starttime
     conn = pyhs2.connect(host='136.159.79.112', port = 10000, authMechanism = "PLAIN", user='wrahman',
     database='default')
     
     cur = conn.cursor()
     cur.execute(ajob)
     # update job status
     #for i in cur.fetch():
     #    print i
     cur.close()
     conn.close()"""

