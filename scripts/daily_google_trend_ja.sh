jobid=$(date +%Y%m%d%H)
lang="ja"

cd ../src

LOG_DIR="log/daily/google_trend/$lang"

# 디렉터리 확인 및 생성
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    echo "로그 디렉터리를 생성했습니다: $LOG_DIR"
else
    echo "로그 디렉터리가 이미 존재합니다: $LOG_DIR"
fi

nohup /data1/share/anaconda3/envs/contents/bin/python -u -m jobs.google_trend.collect_google_trend --lang ${lang} > ${LOG_DIR}/${jobid}.out &