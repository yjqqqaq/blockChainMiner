#!/bin/sh

# This script starts the database server and runs a series of operation against server implementation.
# If the server is implemented correctly, the output (both return values and JSON block) will match the expected outcome.
# Note that this script does not compare the output value, nor does it compare the JSON file with the example JSON.

# Please start this script in a clean environment; i.e. the server is not running, and the data dir is empty.

if [ ! -f "config.json" ]; then
	echo "config.json not in working directory. Trying to go to parent directory..."
	cd ../
fi
if [ ! -f "config.json" ]; then
  	echo "!! Error: config.json not found. Please run this script from the project root directory (e.g. ./example/test_run.sh)."
	exit -1
fi

rm -rf ./tmp
mkdir ./tmp
mkdir ./tmp/Server01
mkdir ./tmp/Server02
mkdir ./tmp/Server03

echo "Testrun starting...(clean start)"

echo "Runing servers"
nohup ./start01.sh &> nohup1.txt&
PID1=$!
sleep 1

nohup ./start02.sh &> nohup2.txt&
PID2=$!
sleep 1

nohup ./start03.sh &> nohup3.txt&
PID3=$!

sleep 1

echo $PID1
echo $PID2
echo $PID3

echo "Waiting for whole start up"
sleep 10

echo "Test Transfer quickly"
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -jar target/blockdb-1.0-SNAPSHOT.jar transfer test000$I test1111 100 5
done

echo "Test Transfer slowly with server crach"

for I in `seq 0 2`; do
	# shellcheck disable=SC2093
	java -jar target/blockdb-1.0-SNAPSHOT.jar transfer test000$I test1111 100 5
	sleep 4
done

echo "Restart Server01"
kill $PID1

nohup ./start01.sh &> nohup1.txt&
PID1=$!
sleep 1

echo "Test Transfer slowly with server crach"

for I in `seq 3 6`; do
	# shellcheck disable=SC2093
	java -jar target/blockdb-1.0-SNAPSHOT.jar transfer test000$I test1111 100 5
	sleep 4
done

echo "Restart Server02"
kill $PID2

nohup ./start02.sh &> nohup2.txt&
PID2=$!
sleep 1


echo "Test Transfer slowly with server crach"

for I in `seq 7 9`; do
	# shellcheck disable=SC2093
	java -jar target/blockdb-1.0-SNAPSHOT.jar transfer test000$I test1111 100 5
	sleep 4
done

echo "Restart Server03"
kill $PID3
nohup ./start03.sh &> nohup3.txt&
PID3=$!
sleep 1

echo "test get"
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -jar target/blockdb-1.0-SNAPSHOT.jar get test000$I
done

# shellcheck disable=SC2093
java -jar target/blockdb-1.0-SNAPSHOT.jar get test1111

echo "Test getHeight"
# shellcheck disable=SC2093
java -jar target/blockdb-1.0-SNAPSHOT.jar getHeight > testResult.txt
cat testResult.txt

echo "Test invalid transfer"

java -jar target/blockdb-1.0-SNAPSHOT.jar transfer test0020 test1111 100 200


java -jar target/blockdb-1.0-SNAPSHOT.jar transfer test0030 test1211 10000 200


java -jar target/blockdb-1.0-SNAPSHOT.jar get Server01
java -jar target/blockdb-1.0-SNAPSHOT.jar get Server02
java -jar target/blockdb-1.0-SNAPSHOT.jar get Server03

kill $PID3
kill $PID2
kill $PID1
