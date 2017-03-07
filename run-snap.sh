h=45.63.51.251
mysql --host=$h -uroot -ptest -e "TRUNCATE __a" __percona

for i in {0..99}
do
mysql --host=$h -uroot -ptest -e "INSERT INTO __a VALUES ($i,100)" __percona
done

for i in {0..4}
do
./start --host=104.238.180.63 > ${i}.out &
done
for i in {5..9}
do
./start --host=45.63.51.251 > ${i}.out &
done
for i in {10..14}
do
./start --host=45.63.83.106 > ${i}.out &
done

wait
