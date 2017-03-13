import mysql.connector
from mysql.connector import errorcode
import sys
import random
import time
from multiprocessing import Process, Value, Lock, Condition

hosts=['vps121034.vps.ovh.ca','vps121033.vps.ovh.ca']
threads=2
accounts=4


def worker(myid, val, cond):
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
    for i in range(0, 10000):

        print "T %d do trx %d" % (myid,i)

        try:
            cur.execute("BEGIN")

            fromAccnt = random.randint(0, accounts-1)
            toAccnt = fromAccnt

            while toAccnt==fromAccnt:
                toAccnt = random.randint(0, accounts-1)

            cur.execute("SELECT val FROM __c WHERE id=%d FOR UPDATE" % (fromAccnt,))
            for row in cur.fetchall():
                fromAmnt=row[0]

            cur.execute("SELECT val FROM __c WHERE id=%d FOR UPDATE" % (toAccnt,))
            for row in cur.fetchall():
                toAmnt=row[0]

            if fromAmnt > 1:
                amt = random.randint(1, fromAmnt-1)

                cur.execute("UPDATE __c SET val = %d WHERE id=%d" % (toAmnt + amt, toAccnt))
                cur.execute("UPDATE __c SET val = %d WHERE id=%d" % (fromAmnt - amt, fromAccnt))

            print "T %d Commit" % (myid,)
            con.commit()
            print "T %d Commit OK" % (myid,)
        except mysql.connector.Error as err:
            print "T %d, Error code: %d" %(myid,err.errno)
            if err.errno==1213:
                con.rollback()

        with val.get_lock():
            val.value += 1

        cond.acquire()
        cond.wait()
        cond.release()


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

      for x in range(0, accounts):
        s = "INSERT INTO __c (id, val) VALUES (%d, 1000)" % (x)
        print "Inserting: ", s
        cur.execute(s)

      con.commit()

    except mysql.connector.Error as err:
        print(err)

    cur.close()
    con.close()

    jobs = []

    v = Value('i', 0)
    cond = Condition()

    try:
        for x in range(0, threads):
            p=Process(target=worker, args=(x,v,cond))
            jobs.append(p)
            p.start()


        for i in range(0,10000):
            while v.value < (threads):
                time.sleep(0.01)

            cond.acquire()
            cond.notify_all()
            cond.release()
            with v.get_lock():
                v.value = 0


        for j in jobs:
            j.join()
    except Exception as error:
        sys.exit(1)
