jobid=$(date +%Y%m%d%H)
lang="ja"
service="google"
suggest_type="basic"
LOG_DIR="log/daily/$suggest_type/$lang/$service/suggest"

cd ../src

# 디렉터리 확인 및 생성
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    echo "로그 디렉터리를 생성했습니다: $LOG_DIR"
else
    echo "로그 디렉터리가 이미 존재합니다: $LOG_DIR"
fi

nohup /data1/anaconda3/envs/contents/bin/python -u -m jobs.basic.get_suggest --lang ${lang} --service ${service} > ${LOG_DIR}/${jobid}.out 2>&1 &