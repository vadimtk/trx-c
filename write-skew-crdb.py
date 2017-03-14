import psycopg2
import sys
import time
from multiprocessing import Process, Value, Lock, Condition

hosts=['host1','host1']
threads=10

def worker(myid, val, cond):
    try:
      con = psycopg2.connect(database='__percona',
              user='postgres',
              host=hosts[myid % len(hosts)],
              password='test', port=26257)
      con.set_session(autocommit=True)

    except:
        print(err)
        sys.exit(1)

    cur = con.cursor()
    for i in range(0, 1000):
        try:
            cur.execute("SELECT SUM(val) av FROM __c")
            rows = cur.fetchall()
            for row in rows:
                if row[0]>1:
                    print("Constraint failed, exit")
                    raise Exception('Contraint violated')
        except Exception as err:
            print "T %d, Error code: %s" %(myid,err)
            con.rollback()

        cur.execute("UPDATE __c SET val = 0 WHERE id=%d" % (myid))

        with val.get_lock():
            val.value += 1

        print "T %d sync wait %d" % (myid,val.value)

        cond.acquire()
        cond.wait()
        cond.release()
        print "T %d sync wait done %d" % (myid,val.value)

        try:
            print "T %d BEGIN" % (myid,)
            cur.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            print "T %d UPDATE" % (myid,)
            cur.execute("UPDATE __c SET val = 1 WHERE id=%d" % (myid))
            print "T %d SELECT" % (myid,)
            cur.execute("SELECT SUM(val) av FROM __c")
            print "T %d fetchall" % (myid,)
            rows = cur.fetchall()
            print "T %d fetchall done" % (myid,)
            for row in rows:
                if row[0] > 1:
                    print "T %d Constraint violated, rollback" % (myid,)
                    con.rollback()
                    continue
            print "T %d Commit" % (myid,)
            con.commit()
            print "T %d Commit OK" % (myid,)
        except Exception as err:
            print "T %d, Error code: %s" %(myid,err)
            con.rollback()

    cur.close()
    con.close()


if __name__ == '__main__':
    try:
      con = psycopg2.connect(database="__percona", user="postgres", host=hosts[0], password="test", port=26257)
      con.set_session(autocommit=True)

    except Exception as err:
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

    except Exception as err:
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
                #print "M %d sync %d" % (v.value,threads)
                time.sleep(0.01)

            print "M %d sync done %d" % (v.value,threads)
            cond.acquire()
            cond.notify_all()
            cond.release()
            with v.get_lock():
                v.value = 0

        for j in jobs:
            j.join()

    except Exception as error:
        sys.exit(1)



        for j in jobs:
            j.join()
    except Exception as error:
        sys.exit(1)
