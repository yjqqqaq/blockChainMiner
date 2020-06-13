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

mkdir ./tmp

echo "Testrun starting...(clean start)"

./startClean.sh &
PID=$!
sleep 1

echo "Test Put..."
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient put test000$I 10
done

echo "Restart server"

kill $PID

./start.sh &
PID=$!
sleep 1

echo "Get put result...(Should be 10) "

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test000$I
done

echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

echo "Test deposit"

for I in `seq 0 4`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done

echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

for I in `seq 5 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done

echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

echo "Check deposit result(should be i + 10) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test000$I
done


echo "Check withdraw operation(test0000 to test0004 should fail)"

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient withdraw test000$I 15
done

echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

echo "Check withdraw operation(test0005 to test0009 should fail)"

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient withdraw test000$I 10
done

echo "Check result (should be 0 1 2 3 4 0 1 2 3 4)"

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test000$I
done
echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1


echo "check transfer to new user, amount = 3, only test0003, test0004, test0008 and test0009 can success"

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient transfer test000$I test1111 3
done

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test000$I
done

echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

echo "check test1111 (should be 12)"
java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test1111

echo "check corrupted json block situation(I will delete last few characters of this json block)"

sed 's/.//' -i ./tmp/1.json


echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

echo "check balance of test1111, should be 0 cause this block is broken"
java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test1111

echo "Check deposit many times (first time) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done


echo "Check deposit many times (second round) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done


echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1


echo "Check deposit many times (third round) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done



echo "Check deposit many times (fourth round) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done



echo "Check deposit many times (fifth round) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done




echo "Check deposit many times (sixth round) "
for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient deposit test000$I $I
done

echo "check log length (should be less or equal to 50) "
java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient logLength

echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1

echo "check multiple deposit result(should be 6 * i) "

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test000$I
done

echo "check corrupted json block situation(I will delete last few characters of this json block)"

sed 's/.//' -i ./tmp/2.json


echo "Restart server"
kill $PID

./start.sh &
PID=$!
sleep 1
echo "check multiple deposit result(should be 5 * i, cause only the last json is broken) "

for I in `seq 0 9`; do
	# shellcheck disable=SC2093
	java -cp ./target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockDatabaseClient get test000$I
done

kill $PID
