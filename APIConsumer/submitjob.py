import pyhs2

def newjob(ajob):

  conn = pyhs2.connect(host='136.159.79.112', port = 10000, authMechanism = "PLAIN", user='wrahman', password='suman@ASE13', database='default')

  cur = conn.cursor()
  cur.execute(ajob)

  #for i in cur.fetch():
  #    print i

  
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

