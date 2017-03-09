# create database __percona;
# create table __b (id int auto_increment primary key, val int);
# create table __proc (procid int primary key, val int); 
MYSQL_PWD=test mysql -h127.0.0.1 -uroot -e "DROP TABLE IF EXISTS __aux; CREATE TABLE __aux ( id int(11) NOT NULL PRIMARY KEY)" __percona
#for i in `seq 0 50 100000`
#do
#        MYSQL_PWD=test mysql -h127.0.0.1 -uroot -e "INSERT INTO __aux VALUES ($i)" __percona
#done
# validation SQL select * from (select floor(z/2) a,count(*) cnt from (select distinct floor(__aux.id/50) z from __b,__aux where val>=__aux.id and val<__aux.id+50 limit 1000) t group by a) t2 where t2.cnt > 1;

MYSQL_PWD=test mysql -h127.0.0.1 -uroot -e "TRUNCATE __b" __percona
MYSQL_PWD=test mysql -h127.0.0.1 -uroot -e "TRUNCATE __proc" __percona
for i in {1..10}
do
        MYSQL_PWD=test mysql -h127.0.0.1 -uroot -e "INSERT INTO __proc (procid) VALUES ($i)" __percona
done
for i in {1..5}
do
./start-G2 --host=10.10.7.167 --procid=$i > $i.out &

done
for i in {6..10}
do
./start-G2 --host=10.10.7.167 --procid=$i > $i.out &

done
wait
