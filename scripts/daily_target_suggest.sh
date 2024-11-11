jobid=$(date +%Y%m%d%H)

cd ../src
nohup /data1/anaconda3/envs/contents/bin/python -u -m jobs.target.get_suggest > log/daily/target/suggest/${jobid}.out 2>&1 &