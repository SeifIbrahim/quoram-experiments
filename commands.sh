# cats the stdout of the first running java application
cat /proc/`jps | awk 'NR==1{print $1}'`/fd/1
