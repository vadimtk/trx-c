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
