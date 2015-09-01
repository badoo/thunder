#!/bin/sh -x
go build

cp thunder thunder-test

rm -rf tmp-source tmp-target offsets.db offsets.db.tmp

mkdir -p tmp-source tmp-target
./thunder-test logs-processor --listen-address='127.0.0.1:7357' --target-dir='tmp-target' &
sleep 0.3
./thunder-test logs-collector --offsets-db='offsets.db' --target-address='127.0.0.1:7357' --source-dir='tmp-source' &
sleep 0.3

echo Test1 >> tmp-source/test.log
echo Test2 >> tmp-source/test.log

sleep 0.3

SUCCESS=1

if ! test "`cat tmp-source/test.log`" = "`cat tmp-target/test.log`"; then
    echo 'Test failed! Expected contents do not match source'
    SUCCESS=0
fi

killall thunder-test

sleep 0.3

rm -rf tmp-source tmp-target offsets.db offsets.db.tmp thunder-test

set +x

echo
echo

if test "$SUCCESS" -eq 0; then
    echo 'Test did not pass! Something went wrong, see errors above'
    exit 1
else
    echo Smoke test passed
fi
