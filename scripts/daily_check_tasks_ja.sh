date=$(date +%Y%m%d)
lang="ja"
LOG_DIR="log/daily/check_tasks/$lang"

cd ../src

# 디렉터리 확인 및 생성
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    echo "로그 디렉터리를 생성했습니다: $LOG_DIR"
else
    echo "로그 디렉터리가 이미 존재합니다: $LOG_DIR"
fi

nohup /data1/share/anaconda3/envs/contents/bin/python -u -m jobs.task_monitor.check_daily_tasks --lang ${lang} --date ${date} > ${LOG_DIR}/${date}.out 2>&1 &