date=$(date --date='14 hours ago' +%Y%m%d)
lang="en"

echo "date: ${date}"
cd ../src

nohup /data1/share/anaconda3/envs/contents/bin/python -u -m jobs.task_monitor.check_daily_tasks --lang ${lang} --date ${date} &
