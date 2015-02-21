python factorial.py > job
cat job | parallel -u --joblog results.log --slf routerlab-resources
