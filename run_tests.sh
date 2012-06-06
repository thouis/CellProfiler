#!/bin/sh

testdirs=$(find . -type d -name tests)

for d in $testdirs; do
    output=$(echo $d | sed "s;/;_;g;s/^..//;s/$/.log/")
    pythonw32 -u cpnose.py --noguitests --exe -v $d 2>&1 | tee $output
done
