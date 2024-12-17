# Trend Finder - Data Collection
Trend Finder에서 사용되는 데이터 수집 자동화

## Schedules
[schedule](https://docs.google.com/spreadsheets/d/15F4ffrwiZFPwN90mLG6wrei7hrBnLi-Ziq2JUhoWdQg/edit?gid=0#gid=0)

## Usage
대상키워드 수집 예시
```
# target-한국-구글
1 0 * * * cd /data2/projects/ds-trend-finder-collector/scripts ; source ./daily_target_ko_google_suggest.sh
10 0 * * * cd /data2/projects/ds-trend-finder-collector/scripts ; source ./daily_target_ko_google_serp.sh
# target-일본-구글
1 0 * * * cd /data2/projects/ds-trend-finder-collector/scripts ; source ./daily_target_ja_google_suggest.sh
10 0 * * * cd /data2/projects/ds-trend-finder-collector/scripts ; source ./daily_target_ja_google_serp.sh
# target-미국-구글
1 14 * * * cd /data2/projects/ds-trend-finder-collector/scripts ; source ./daily_target_en_google_suggest.sh
10 14 * * * cd /data2/projects/ds-trend-finder-collector/scripts ; source ./daily_target_en_google_serp.sh
```
