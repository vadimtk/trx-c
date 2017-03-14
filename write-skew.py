import mysql.connector
from mysql.connector import errorcode
import sys
from multiprocessing import Process

hosts=['vps121034','vps121033']
threads=2

def worker(myid):
    try:
      con = mysql.connector.connect(user='root', password='test',
                      host=hosts[myid % len(hosts)],
                      database='__percona',
                      autocommit=True)

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
      sys.exit(1)

    cur = con.cursor()
    for i in range(0,1000):
        cur.execute("SELECT SUM(val) av FROM __c")
        for row in cur.fetchall():
            if row[0]>1:
                print("Constraint failed, exit")
                raise Exception('Contraint violated')

        cur.execute("UPDATE __c SET val = 0 WHERE id=%d" % (myid))

        print "T %d sync wait" % (myid,)
        cond = True
        while cond:
            cur.execute("SELECT SUM(val) av FROM __c")
            for row in cur.fetchall():
                # print "T %d sync res %d" % (myid,row[0])
                if row[0]>1:
                    print("Constraint failed, exit")
                    raise Exception('Contraint violated')
                cond=(row[0]>0)

        print "T %d do trx %d" % (myid,i)

        try:
            cur.execute("BEGIN")
            cur.execute("UPDATE __c SET val = 1 WHERE id=%d" % (myid))
            cur.execute("SELECT SUM(val) av FROM __c FOR UPDATE")
            for row in cur.fetchall():
                if row[0] > 1:
                    print "T %d Constraint violated, rollback" % (myid,)
                    con.rollback()
                else:
                    print "T %d Commit" % (myid,)
                    con.commit()
                    print "T %d Commit OK" % (myid,)
        except mysql.connector.Error as err:
            print "T %d, Error code: %d" %(myid,err.errno)
            if err.errno==1213:
                con.rollback()

    cur.close()
    con.close()


if __name__ == '__main__':
    try:
      con = mysql.connector.connect(user='root', password='test',
                                  host=hosts[0],
                                  database='__percona')

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
      sys.exit(1)

    # prep work - create table and initiate it
    try:
      cur = con.cursor()

      cur.execute("DROP TABLE IF EXISTS __c")
      cur.execute("CREATE TABLE __c (id int primary key, val int)")

      for x in range(0, threads):
        s = "INSERT INTO __c (id, val) VALUES (%d, 0)" % (x)
        print "Inserting: ", s
        cur.execute(s)

      con.commit()

    except mysql.connector.Error as err:
        print(err)

    cur.close()
    con.close()

    jobs = []
    try:
        for x in range(0, threads):
            p=Process(target=worker, args=(x,))
            jobs.append(p)
            p.start()
        for j in jobs:
            j.join()
    except Exception as error:
        sys.exit(1)
