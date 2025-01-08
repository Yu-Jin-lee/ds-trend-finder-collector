import os
import time
import argparse
from typing import List
from datetime import datetime

from utils.file import TXTFileHandler, JsonlFileHandler, GZipFileHandler
from collector.serp_collector.serp_collector import SerpCollector
from utils.task_history import TaskHistory
from utils.hdfs import HdfsFileHandler
from utils.slack import ds_trend_finder_dbgout, ds_trend_finder_dbgout_error
from utils.decorator import error_notifier
from config import postgres_db_config
from serp.serp_checker import SerpChecker, SerpCheckerKo, SerpCheckerJa, SerpCheckerEn

def get_keywords_already_collected_serp(
                                        lang:str, # ["ko", "ja"]
                                        date:str # yyyymmdd
                                        ) -> List[str]:
    '''
    hdfs에서 해당 날짜에 서프가 이미 수집된 키워드 목록 가져오기
    '''
    hdfs = HdfsFileHandler()
    services = ["google", 'youtube']
    suggest_types = ["basic", "target"]
    keywords = []
    for service in services:
        for suggest_type in suggest_types:
            file_path = f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_llm_entity_topic/{date[:4]}/{date[:6]}/{date[:8]}/serp_keywords_{suggest_type}.txt"
            if not hdfs.exist(file_path):
                print(f"[{datetime.now()}] 해당 hdfs 경로에 파일이 존재하지 않습니다. (file_path : {file_path})")
                continue
            contents = hdfs.load(file_path)
            keywords += list(set([line.strip() for line in contents.splitlines() if line.strip()]))  # 중복 제거 및 정렬
    return keywords

class EntitySerpDaily:
    def __init__(self, job_id:str, lang:str, service:str, log_task_history:bool=False):
        # 기본 정보
        self.job_id = job_id
        self.lang = lang
        self.service = service
        self.project_name = "trend_finder"
        self.suggest_type = "basic"
        self.serp_collector = SerpCollector(lang)

        # local 관련
        self.local_folder_path = f"./data/result/{self.suggest_type}/{self.service}/{self.lang}"
        if not os.path.exists(self.local_folder_path):
            os.makedirs(self.local_folder_path)
        self.trend_keyword_file = f"{self.local_folder_path}/{self.job_id}_trend_keywords_new.txt"
        self.serp_download_local_path = f"{self.local_folder_path}/{self.job_id}_serp.jsonl"
        self.serp_download_local_path_non_domestic = f"{self.local_folder_path}/{self.job_id}_serp_non_domestic.jsonl"

        # hdfs 관련
        self.hdfs = HdfsFileHandler()
        self.hdfs_upload_folder = f"/user/ds/wordpopcorn/{self.lang}/daily/{self.service}_suggest_for_llm_entity_topic/{self.job_id[:4]}/{self.job_id[:6]}/{self.job_id[:8]}/{self.job_id}"
        self.hdfs_already_collected_serp_keywords_path = f"/user/ds/wordpopcorn/{self.lang}/daily/{self.service}_suggest_for_llm_entity_topic/{self.job_id[:4]}/{self.job_id[:6]}/{self.job_id[:8]}/serp_keywords_{self.suggest_type}.txt"
       
        # Task History 관련
        self.log_task_history = log_task_history
        self.task_name = f"수집-서프-{service}-{self.suggest_type}"
        self.task_history_updater = TaskHistory(postgres_db_config, self.project_name, self.task_name, self.job_id, self.lang)
        
        # slack 관련
        self.slack_prefix_msg = f"Job Id : `{self.job_id}`\nTask Name : `{self.task_name}`-`{self.lang}`"
        
        # serp_checker 관련
        self.serp_checker = self.get_serp_checker()

    @error_notifier
    def append_keywords_to_serp_keywords_txt(self, keywords, log:bool=True):
        '''
        hdfs에 저장된 serp_keywords_target.txt에 키워드 추가
        '''
        # 키워드 리스트를 문자열로 변환 (줄바꿈으로 각 키워드 구분)
        keyword_data = '\n'.join(keywords) + '\n'

        # 파일이 존재하는지 확인하고, 없으면 새로 생성
        if not self.hdfs.exist(self.hdfs_already_collected_serp_keywords_path):
            # 파일이 존재하지 않을 경우 새로 생성
            self.hdfs.write(self.hdfs_already_collected_serp_keywords_path, keyword_data, encoding='utf-8')
        else:
            # 파일이 존재할 경우, append 모드로 키워드 추가
            self.hdfs.write(self.hdfs_already_collected_serp_keywords_path, keyword_data, encoding='utf-8', append=True)
        if log:
            print(f"[{datetime.now()}] hdfs에 키워드 추가 완료 (keywords : {len(keywords)}개 키워드, hdfs_path : {self.hdfs_already_collected_serp_keywords_path})")

    @error_notifier
    def get_serp_checker(self) -> SerpChecker:
        if self.lang == "ko":
            return SerpCheckerKo
        elif self.lang == "ja":
            return SerpCheckerJa
        elif self.lang == "en":
            return SerpCheckerEn
        else:
            raise ValueError(f"지원하지 않는 언어입니다. (lang : {self.lang})")
        
    @error_notifier
    def collect_serp(self, keywords : List[str]):
        batch_size = 100
        error_keywords_len = 999

        while error_keywords_len > 0: # 에러 키워드가 없을 때까지 반복
            error_keywords = []
            for i in range(0, len(keywords), batch_size):
                print(f"[{datetime.now()}] {i}/{len(keywords)}")
                res, error_res = self.serp_collector.get_serp_from_serp_api(keywords[i:i+batch_size], 
                                                                            domain="llm_entity_topic")
                # 해당 국가의 서프 구분
                domestic_serps = []
                non_domestic_serps = []
                for r in res:
                    if self.is_domestic_serp(r): domestic_serps.append(r)
                    else: non_domestic_serps.append(r)
                
                print(f"[{datetime.now()}] domestic_serps : {len(domestic_serps)}/{len(res)}개, non_domestic_serps : {len(non_domestic_serps)}/{len(res)}개")

                JsonlFileHandler(self.serp_download_local_path).write(domestic_serps)
                JsonlFileHandler(self.serp_download_local_path_non_domestic).write(non_domestic_serps)

                error_keywords += [r[0]['query'] for r in error_res if (len(r)>0 and type(r[0])==dict and 'query' in r[0])]
            error_keywords_len = len(error_keywords) # 에러 키워드 수 갱신
            keywords = error_keywords # 에러 키워드로 다시 수집
            print(f"[{datetime.now()}] 서프 수집 에러 키워드 : {error_keywords_len} 개")
        print(f"[{datetime.now()}] 서프 수집 완료 (local dest : {self.serp_download_local_path})")

    @error_notifier
    def is_domestic_serp(self, serp:dict) -> bool:
        return self.serp_checker(serp).is_domestic()
    
    @error_notifier
    def count_line(self, path) -> int:
        if path.endswith(".gz"): # 압축 파일이면 압축해제
            read_path = GZipFileHandler.ungzip(path)
        else:
            read_path = path
        lines = JsonlFileHandler(read_path).count_line()
        if (path != read_path and 
            ~read_path.endswith(".gz")): # 압축 풀었던 파일 다시 압축
            GZipFileHandler.gzip(read_path)
        return lines
    
    @error_notifier
    def extract_statistics(self):
        '''
        수집 결과에서 통계 정보 추출
        '''
        domestic_serp_count = self.count_line(self.serp_download_local_path)
        non_domestic_serp_count = self.count_line(self.serp_download_local_path_non_domestic)
        return {
            "total": domestic_serp_count + non_domestic_serp_count,
            "domestic": domestic_serp_count,
            "non_domestic": non_domestic_serp_count
        }
    
    @error_notifier
    def upload_to_hdfs(self):
        if os.path.exists(self.serp_download_local_path):
            basic_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}_serp.jsonl.gz"
            self.hdfs.upload(source=self.serp_download_local_path, dest=basic_hdfs_path, overwrite=True)
        else:
            print(f"[{datetime.now()}] error from upload_to_hdfs : 로컬에 해당 파일이 존재하지 않습니다 (serp_download_local_path : {self.serp_download_local_path})")
        if os.path.exists(self.serp_download_local_path_non_domestic):
            non_domestic_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}_serp_non_domestic.jsonl.gz"
            self.hdfs.upload(source=self.serp_download_local_path_non_domestic, dest=non_domestic_hdfs_path, overwrite=True)
        else:
            print(f"[{datetime.now()}] error from upload_to_hdfs : 로컬에 해당 파일이 존재하지 않습니다 (serp_download_local_path_non_domestic : {self.serp_download_local_path_non_domestic})")

    def run(self):
        try:
            if self.log_task_history:
                self.task_history_updater.set_task_start()
                self.task_history_updater.set_task_in_progress()
            # 파일이 존재할 때까지 기다림
            print(f"[{datetime.now()}] 키워드 파일의 생성을 기다리는 중입니다: {self.trend_keyword_file}")
            while not os.path.exists(self.trend_keyword_file):
                time.sleep(60)  # 60초마다 파일 존재 여부를 확인
            print(f"[{datetime.now()}] 키워드 파일이 생성되었습니다. 프로세스를 시작합니다.")

            last_keyword_count = 0  # 마지막으로 처리한 키워드 수
            no_new_keywords_count = 0  # 새 키워드가 없었던 횟수
            
            # 새 키워드가 없는 것을 몇 번 확인할지 설정
            max_no_new_keywords_count = 15
            
            already_collected_keywords = set() # 이미 수집한 키워드
            if os.path.exists(self.serp_download_local_path): # 서프 수집한 결과 있으면 추가
                for serp in JsonlFileHandler(self.serp_download_local_path).read_generator():
                    if serp == []:
                        continue
                    already_collected_keywords.add(serp['search_parameters']['q'])
            while no_new_keywords_count < max_no_new_keywords_count:
                # 키워드를 읽음
                trend_keywords = TXTFileHandler(self.trend_keyword_file).read_lines()
                
                # 새로운 키워드 확인
                if len(trend_keywords) > last_keyword_count:
                    print(f"[{datetime.now()}] 새로운 키워드 발견! 수집을 시작합니다. (키워드 개수: {len(trend_keywords)})")
                    
                    # 새로운 키워드를 처리
                    keywords_to_collect_serp = list(set(trend_keywords[last_keyword_count:]) - already_collected_keywords)
                    print(f"[{datetime.now()}] 이미 수집된 키워드 제거 후 1 ({len(keywords_to_collect_serp)})개")
                    keywords_to_collect_serp = list(set(keywords_to_collect_serp) - set(get_keywords_already_collected_serp(self.lang, self.job_id))) # 오늘 수집한 키워드 제외
                    print(f"[{datetime.now()}] 이미 수집된 키워드 제거 후 2 ({len(keywords_to_collect_serp)})개")
                    self.collect_serp(keywords_to_collect_serp) # 이미 수집한 키워드 제외하고 수집
                    self.append_keywords_to_serp_keywords_txt(keywords_to_collect_serp) # 수집한 키워드 hdfs에 저장
                    already_collected_keywords = set(list(already_collected_keywords) + trend_keywords[last_keyword_count:])
                    
                    # 마지막으로 처리한 키워드 수 업데이트
                    last_keyword_count = len(trend_keywords)
                    no_new_keywords_count = 0  # 새 키워드가 있으면 카운트를 초기화
                    
                else:
                    # 새로운 키워드가 없으면 카운트를 증가
                    no_new_keywords_count += 1
                    print(f"[{datetime.now()}] 새로운 키워드가 없습니다. {no_new_keywords_count}/{max_no_new_keywords_count} 번째 대기 중...")
                    
                # 주기적으로 대기 (파일이 다시 채워질 수 있도록 대기)
                time.sleep(60*1)  # 1분마다 파일을 확인

            print(f"[{datetime.now()}] 더 이상 키워드가 추가되지 않아 프로세스를 종료합니다.")

            # 압축
            self.serp_download_local_path = GZipFileHandler.gzip(self.serp_download_local_path)
            self.serp_download_local_path_non_domestic = GZipFileHandler.gzip(self.serp_download_local_path_non_domestic)

            # HDFS 업로드
            self.upload_to_hdfs()
            
            if self.log_task_history:
                self.task_history_updater.set_task_completed()
        except Exception as e:
            print(f"[{datetime.now()}] 서프 수집 실패 작업 종료\nError Msg : {e}")
            ds_trend_finder_dbgout_error(self.lang,
                                         f"{self.slack_prefix_msg}\nMessage : 서프 수집 실패 작업 종료")
            if self.log_task_history:
                self.task_history_updater.set_task_error(error_msg=str(e))
        else:
            print(f"[{datetime.now()}] 서프 수집 완료")
            statistic = self.extract_statistics()
            ds_trend_finder_dbgout(self.lang,
                                   f"{self.slack_prefix_msg}\nMessage : 서프 수집 완료\nUpload Path : {self.hdfs_upload_folder}\nStatistics : (total: {statistic['total']} | domestic: {statistic['domestic']} | non_domestic: {statistic['non_domestic']})")
        
def find_last_job_id(suggest_type, lang, service, today):
    data_path = f"./data/result/{suggest_type}/{service}/{lang}"
    file_list = os.listdir(data_path)
    file_list.sort()
    last_file = file_list[-1]
    job_id = last_file[:10]
    return job_id
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", help="language", default=None)
    parser.add_argument("--service", help="service(google or youtube)", default=None)
    args = parser.parse_args()
    
    pid = os.getgid()
    print(f"pid : {pid}")

    suggest_type = "basic"
    today = datetime.now().strftime("%Y%m%d")
    job_id = find_last_job_id(suggest_type, args.lang, args.service, today)
    print(f"job_id : {job_id}")
    
    print(f"---------- [{datetime.now()}] {args.lang} {args.service} 수집 시작 ----------")
    entity_serp_daily = EntitySerpDaily(job_id, args.lang, args.service, log_task_history=True)
    entity_serp_daily.run()
    print(f"---------- [{datetime.now()}] {args.lang} {args.service} 수집 완료 ----------")