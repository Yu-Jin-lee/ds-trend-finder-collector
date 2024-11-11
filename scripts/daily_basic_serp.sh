jobid=$(date +%Y%m%d%H)

cd ../src
nohup /data1/anaconda3/envs/contents/bin/python -u -m jobs.basic.get_serp > log/daily/basic/serp/${jobid}.out 2>&1 &