import argparse
from datetime import datetime

from utils.hdfs import HdfsFileHandler
from utils.slack import ds_trend_finder_dbgout, ds_trend_finder_dbgout_error, flag_emoji

class DailyTasksMonitor:
    def __init__(self, lang:str, date:str):
        self.lang = lang
        self.date = date
        self.services = ['google', 'youtube']
        self.hdfs = HdfsFileHandler()
    
    def get_schedule(self, lang:str) -> dict:
        '''
        해당 언어의 스케줄을 가져옴
        '''
        schedule = {
            "ko" : {
                "basic" : {
                    "google" : [0, 12],
                    "youtube" : [6, 18]
                },
                "target" : {
                    "google" : [0],
                    "youtube" : []
                }
            },
            "ja" : {
                "basic" : {
                    "google" : [2, 14],
                    "youtube" : [8, 20]
                },
                "target" : {
                    "google" : [0],
                    "youtube" : []
                }
            },
            "en" : {
                "basic" : {
                    "google" : [14-14, 26-14],
                    "youtube" : [18-14, 30-14]
                },
                "target" : {
                    "google" : [14-14],
                    "youtube" : []
                }
            }
        }

        return schedule[lang]

    def load_keywords_from_hdfs(self, file_path) -> set:
        """HDFS에서 txt 파일을 읽어와서 키워드 리스트로 반환"""
        try:
            with self.hdfs.client.read(file_path, encoding="utf-8") as f:
                contents = f.read()
            keywords = set([line.strip() for line in contents.splitlines() if line.strip()])  # 중복 제거 및 정렬
            return sorted(keywords)
        except Exception as e:
            print(f"HDFS에서 파일을 불러올 수 없습니다: {e}")
            return []
    
    def check_collection_files(self) -> tuple:
        '''
        수집 관련 파일들이 모두 hdfs에 업로드 되었는지 확인
        '''
        schedule = self.get_schedule(self.lang)
        all_success_folders = []
        all_success_files = []
        all_failed_folders = []
        all_failed_files = []
        trend_kws = {"google":[], "youtube":[], "target":[], "basic":[]}
        trend_kws_new = {"google":[], "youtube":[], "target":[], "basic":[]}
        for suggest_type, schedule_by_services in schedule.items():
            for service, hours in schedule_by_services.items():
                for hour in hours:
                    job_id = self.date + f"{hour:02}"
                    # hdfs 저장 폴더
                    collect_root_folder = f"/user/ds/wordpopcorn/{self.lang}/daily/{service}_suggest_for_llm_entity_topic/{self.date[:4]}/{self.date[:6]}/{self.date[:8]}/{job_id}"
                    if self.hdfs.exist(collect_root_folder):
                        all_success_folders.append(collect_root_folder)
                        # hdfs에 업로드 되어있는 파일 리스트
                        exist_file_list = self.hdfs.list(collect_root_folder)
                        # 있어야 할 파일 리스트
                        file_suggest = f"{job_id}_{suggest_type}.jsonl.gz" # 서제스트
                        file_serp = f"{job_id}_{suggest_type}_serp.jsonl.gz" # 서프
                        file_trend_keyword = f"{job_id}_{suggest_type}_trend_keywords.txt" # 트렌드 키워드
                        file_trend_keyword_new = f"{job_id}_{suggest_type}_trend_keywords_new.txt" # 새로운 트렌드 키워드
                        check_files = [file_suggest, file_serp, file_trend_keyword, file_trend_keyword_new]
                        # 존재하는 파일
                        success_files = set.intersection(set(check_files), set(exist_file_list))
                        all_success_files.extend(success_files)
                        # 트렌드 키워드 읽기
                        if file_trend_keyword in success_files:
                            trend_kws[service] += self.load_keywords_from_hdfs(f"{collect_root_folder}/{file_trend_keyword}")
                            trend_kws[service] = list(set(trend_kws[service]))
                            trend_kws[suggest_type] += self.load_keywords_from_hdfs(f"{collect_root_folder}/{file_trend_keyword}")
                            trend_kws[suggest_type] = list(set(trend_kws[suggest_type]))
                        if file_trend_keyword_new in success_files:
                            trend_kws_new[service] += self.load_keywords_from_hdfs(f"{collect_root_folder}/{file_trend_keyword_new}")
                            trend_kws_new[service] = list(set(trend_kws_new[service]))
                            trend_kws_new[suggest_type] += self.load_keywords_from_hdfs(f"{collect_root_folder}/{file_trend_keyword_new}")
                            trend_kws_new[suggest_type] = list(set(trend_kws_new[suggest_type]))
                        # 존재하지 않는 파일
                        failed_files = set(check_files) - set(exist_file_list)
                        all_failed_files.extend(list(failed_files))
                    else:
                        all_failed_folders.append(collect_root_folder)
        return all_success_folders, all_success_files, all_failed_folders, all_failed_files, trend_kws, trend_kws_new
    
    def check_analysis_files(self):
        '''
        분석 관련 파일들이 모두 hdfs에 업로드 되었는지 확인
        '''
        schedule = self.get_schedule(self.lang)
        all_success_folders = []
        all_success_files = []
        all_failed_folders = []
        all_failed_files = []
        # hdfs 저장 폴더
        analysis_root_folder = f"/user/ds/wordpopcorn/{self.lang}/daily/issue_data/{self.date[:4]}/{self.date[:6]}/{self.date}"
        for suggest_type, schedule_by_services in schedule.items():
            for service, hours in schedule_by_services.items():
                for hour in hours:
                    job_id = self.date + f"{hour:02}"
                    check_folder = f"{analysis_root_folder}/{job_id}_{service}_{suggest_type}"
                    if self.hdfs.exist(check_folder):
                        all_success_folders.append(check_folder)
                        exist_file_list = self.hdfs.list(check_folder)
                        if service == "google":
                            check_files = ["issue_analysis.tsv"]
                        elif service == "youtube":
                            check_files = ["daily_issue.tsv", "daily_topic.tsv", "issue_analysis.tsv", "serp_snippet.tsv"]
                        # 존재하는 파일
                        success_files = set.intersection(set(check_files), set(exist_file_list))
                        all_success_files.extend(success_files)
                        # 존재하지 않는 파일
                        failed_files = set(check_files) - set(exist_file_list)
                        all_failed_files.extend(list(failed_files))
                    else:
                        all_failed_folders.append(check_folder)

        return all_success_folders, all_failed_folders, all_success_files, all_failed_files
    
    def check(self):
        '''
        데일리로 업로드 되어야 할 모든 파일이 업로드 되었는지 확인
        '''
        collect_success_folders, collect_success_files, collect_failed_folders, collect_failed_files, trend_kws, trend_kws_new = self.check_collection_files()
        analysis_success_folders, analysis_failed_folders, analysis_success_files, analysis_failed_files = self.check_analysis_files()

        if (len(collect_failed_folders) > 0 or
            len(collect_failed_files) > 0 or 
            len(analysis_failed_folders) > 0 or
            len(analysis_failed_files) > 0
            ):
            error_msg = (
                        f"❌{flag_emoji(self.lang)} `{self.lang}` `{self.date}` 모든 작업의 결과 업로드 실패❌\n"
                        f"[업로드 되지 않은 폴더, 파일 리스트]\n"
                        f" ㄴ수집: {collect_failed_folders + collect_failed_files}\n"
                        f" ㄴ분석: {analysis_failed_folders + analysis_failed_files}"
                    )
            print(f"[{datetime.now()}] {error_msg}")
            ds_trend_finder_dbgout_error(self.lang,
                                         error_msg)
        else:
            success_msg = (
                            f"✅{flag_emoji(self.lang)} `{self.lang}` `{self.date}` 모든 작업의 결과 업로드 완료✅\n"
                            f"*[오늘의 트렌드 키워드]*\n"
                            f"  ㄴ전체: total({len(set(trend_kws['google'] + trend_kws['youtube']))}개) | new({len(set(trend_kws_new['google'] + trend_kws_new['youtube']))}개)\n"
                            f" *서비스별*\n"
                            f"  ㄴ구글: total({len((trend_kws['google']))}개) | new({len(trend_kws_new['google'])}개)\n"
                            f"  ㄴ유튜브: total({len(trend_kws['youtube'])}개) | new({len(trend_kws_new['youtube'])}개)\n"
                            f" *서제스트 타입별*\n"
                            f"  ㄴ기본: total({len(trend_kws['basic'])}개) | new({len(trend_kws_new['basic'])}개)\n"
                            f"  ㄴ대상키워드: total({len(trend_kws['target'])}개) | new({len(trend_kws_new['target'])}개)"
                        )
            print(f"[{datetime.now()}] {success_msg}")
            ds_trend_finder_dbgout(self.lang,
                                   success_msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", help="language", default=None)
    parser.add_argument("--date", help="date(yyyymmdd)", default=None)
    args = parser.parse_args()

    daily_tasks_monitor = DailyTasksMonitor(args.lang, args.date)
    daily_tasks_monitor.check()

