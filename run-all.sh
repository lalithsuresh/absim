#!/bin/bash
set -e
python factorial.py > job
> results.log
tail -f results.log &
TAIL_PID=$!
cat job | parallel -u --env PATH --joblog results.log --slf `./which-resources`
kill $TAIL_PID
cat results.log | awk 'NR > 1 { if ($6 + $7 > 0) { print "Problem with job: ", $9 } }'
