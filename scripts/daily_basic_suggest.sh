jobid=$(date +%Y%m%d%H)

cd ../src
nohup /data1/anaconda3/envs/contents/bin/python -u -m jobs.basic.get_suggest > log/daily/basic/suggest/${jobid}.out 2>&1 &