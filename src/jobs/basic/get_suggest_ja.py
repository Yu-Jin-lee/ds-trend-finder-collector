import os
import gc
import math
import argparse
from datetime import datetime, timedelta
from typing import List

from collector.suggest_collector.suggest_collect import Suggest
from validator.trend_keyword_validator import is_trend_keyword, cnt_valid_suggest
from utils.file import JsonlFileHandler, GZipFileHandler, TXTFileHandler, has_file_extension
from utils.data import Trie, remove_duplicates_from_new_keywords, remove_duplicates_from_new_keywords_ko, remove_duplicates_with_spaces
from utils.hdfs import HdfsFileHandler
from utils.task_history import TaskHistory
from utils.slack import ds_trend_finder_dbgout, ds_trend_finder_dbgout_error
from utils.decorator import error_notifier
from utils.converter import adjust_job_id
from lang import Ko, Ja, En, filter_en_valid_trend_keyword, filter_en_valid_token_count
from lang.ja.ja import hiragana, katakana, katakana_chouon, youon, sokuon_hiragana, sokuon_katakana, gairaigo_katakana, alphabet, number, kanji
            
from config import postgres_db_config

class EntitySuggestDaily:
    def __init__(self, lang : str, service : str, job_id:str, log_task_history:bool=False):
        # 기본정보
        self.lang = lang
        self.service = service
        self.project_name = "trend_finder"
        self.suggest_type = "basic"
        self.job_id = job_id

        # local 관련
        self.local_folder_path = f"./data/result/{self.suggest_type}/{self.service}/{self.lang}"
        if not os.path.exists(self.local_folder_path):
            os.makedirs(self.local_folder_path)
        self.trend_keyword_file = f"{self.local_folder_path}/{self.job_id}_trend_keywords.txt"
        self.new_trend_keyword_file = f"{self.local_folder_path}/{self.job_id}_trend_keywords_new.txt" # 새로 나온 트렌드 키워드 저장
        self.except_for_valid_trend_keywords_file = f"{self.local_folder_path}/{self.job_id}_except_for_valid_trend_keywords.txt" # 유효하지 않은 트렌드 키워드 저장
        self.local_result_path = f"{self.local_folder_path}/{self.job_id}.jsonl"
        
        # hdfs 관련
        self.hdfs = HdfsFileHandler()
        self.hdfs_upload_folder = f"/user/ds/wordpopcorn/{self.lang}/daily/{self.service}_suggest_for_llm_entity_topic/{self.job_id[:4]}/{self.job_id[:6]}/{self.job_id[:8]}/{self.job_id}"
        self.past_trend_keywords : List[str] = self.get_past_trend_keywords(self.job_id[:8], lang, 28) # 이전 N일전 나왔던 트렌드 키워드
        
        # Task History 관련
        self.task_name = f"수집-서제스트-{service}-{self.suggest_type}"
        self.log_task_history = log_task_history
        self.task_history_updater = TaskHistory(postgres_db_config, self.project_name, self.task_name, self.job_id, self.lang)

        # slack 관련
        self.slack_prefix_msg = f"Job Id : `{self.job_id}`\nTask Name : `{self.task_name}`-`{self.lang}`"

        # 통계량 관련
        self.statistics = {"call": {}, "valid": {}, "trend_keyword": {}}

    @error_notifier
    def get_lang(self, lang:str):
        if lang == "ko":
            return Ko()
        if lang == "ja":
            return Ja()
        if lang == "en":
            return En()
        else:
            print(f"[{datetime.now()}] {lang} 해당 국가는 지원하지 않습니다. (EntitySuggestDaily)")
            raise ValueError(f"{lang} 해당 국가는 지원하지 않습니다. (EntitySuggestDaily)")
    
    @error_notifier
    def load_keywords_from_hdfs(self, file_path):
        """HDFS에서 txt 파일을 읽어와서 키워드 리스트로 반환"""
        contents = self.hdfs.load(file_path)
        keywords = set([line.strip() for line in contents.splitlines() if line.strip()])  # 중복 제거 및 정렬
        return sorted(keywords)
    
    @error_notifier
    def get_all_txt_files(self, date_folder_path):
        '''
        입력한 date_folder_path 하위 경로를 돌면서 .txt 파일 목록을 가져오는 함수
        '''
        if not self.hdfs.exist(date_folder_path):
            print(f"[{datetime.now()}] message from get_all_txt_files : {date_folder_path} 경로가 존재하지 않습니다.")
            return []
        # 현재 폴더의 하위 디렉토리 목록을 가져옴
        job_id_dirs = [d for d in self.hdfs.list(date_folder_path) if not has_file_extension(d)] # 디렉토리만 가져옴
        all_txt_files = []

        # 하위 디렉토리 목록을 순회
        for job_id in job_id_dirs:
            job_id_path = f"{date_folder_path}/{job_id}"
            files = self.hdfs.list(job_id_path)
            for file in files:
                if file.endswith("_trend_keywords.txt"):
                    all_txt_files.append(f"{job_id_path}/{file}")

        return all_txt_files
    
    @error_notifier
    def get_already_collected_keywords(self) -> List[str]:
        already_collected_keywords = []
        if os.path.exists(self.local_result_path):
            print(f"[{datetime.now()}] 이미 수집된 서제스트 결과가 있습니다. (path : {self.local_result_path})")
            for line in JsonlFileHandler(self.local_result_path).read_generator():
                already_collected_keywords.append(line['keyword'])
            print(f"[{datetime.now()}] ㄴ {len(already_collected_keywords)}개 키워드 수집되어 있음")
        return list(set(already_collected_keywords))
    
    @error_notifier
    def get_past_trend_keywords(self, today:str, lang:str, days:int) -> List[str]:
        '''
        이전 트렌드 키워드 목록 가져오기
        '''
        services = ['google', 'youtube']
                
        print(f"[{datetime.now()}] {today} 기준 이전 {days}일 트렌드 키워드 목록 가져오기")
        today_datetime = datetime.strptime(today, "%Y%m%d")
        past_trend_keywords = []
        for i in range(1, days+1, 1):
            date = today_datetime - timedelta(days=i)
            date = date.strftime("%Y%m%d")
            for service in services:
                txt_files = self.get_all_txt_files(f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_llm_entity_topic/{date[:4]}/{date[:6]}/{date[:8]}")
                for file in txt_files:
                    past_trend_keywords += self.load_keywords_from_hdfs(file)

        past_trend_keywords = list(set(past_trend_keywords))
        print(f"키워드 개수 : {len(set(past_trend_keywords))}")

        return past_trend_keywords
    
    @error_notifier  
    def get_suggest_and_request_serp(self,
                                     targets : List[str],
                                     result_file_path : str, # "*.jsonl"
                                     num_processes:int
                                     ) -> str: # 저장 경로 반환
        '''
        서제스트 수집 요청 및 로컬에 저장 + 서프 수집 요청
        '''
        suggest = Suggest()
        batch_size = 10000
        print(f"[{datetime.now()}] 수집할 개수 : {len(targets)} | batch_size : {batch_size}")
        for i in range(0, len(targets), batch_size):
            start = datetime.now()
            result = suggest._requests(targets[i : i+batch_size], 
                                       self.lang, 
                                       self.service, 
                                       num_processes = num_processes)
            print(f"[{datetime.now()}]    ㄴ batch {int((i+batch_size)/batch_size)}/{math.ceil(len(targets)/batch_size)} finish : {datetime.now()-start}")
            JsonlFileHandler(result_file_path).write(result)
            # 트렌드 키워드 추출
            try:
                trend_keywords = [suggestion['text'] for res in result for suggestion in res['suggestions'] if is_trend_keyword(suggestion['text'], # 트렌드 키워드 추출
                                                                                                                        suggestion['suggest_type'], 
                                                                                                                        suggestion['suggest_subtypes'])]
                valid_trend_keywords = [keyword for keyword in trend_keywords if self.filter_valid_trend_keywords(keyword)] # 트렌드 키워드 중 유효한 키워드만 추출
                print(f"[{datetime.now()}]       ㄴ✔️유효한 트렌드 키워드 개수 : {len(valid_trend_keywords)}/{len(trend_keywords)}")
                TXTFileHandler(self.trend_keyword_file).write(valid_trend_keywords) # valid_trend_keywords 저장
                TXTFileHandler(self.except_for_valid_trend_keywords_file).write(list(set(trend_keywords) - set(valid_trend_keywords))) # valid_trend_keywords를 제외한 나머지 저장
                # 새로 나온 트렌드 키워드 추출
                if self.lang == "ko":
                    new_trend_keywords = list(remove_duplicates_from_new_keywords_ko(set(self.past_trend_keywords), set(valid_trend_keywords)))
                else:
                    new_trend_keywords = list(remove_duplicates_from_new_keywords(set(self.past_trend_keywords), set(valid_trend_keywords)))
                TXTFileHandler(self.new_trend_keyword_file).write(new_trend_keywords)
            except Exception as e:
                print(f"[{datetime.now()}] 트렌드 키워드 추출 및 저장 실패 : {e}")
            # TODO : 서프 수집 요청 (kafka)
            
        return result_file_path

    @error_notifier
    def filter_valid_trend_keywords(self, trend_keyword:str) -> bool:
        '''
        입력된 트렌드 키워드가 유효한 키워드인지 확인
        '''
        if self.lang == "en":
            if filter_en_valid_trend_keyword(trend_keyword) & filter_en_valid_token_count(trend_keyword): return True
            else: return False
        else:
            return True
    
    @error_notifier
    def get_1st_extension(self) -> List[str]:
        '''
        1단계 확장 텍스트 가져오기
        '''
        extension_rank_1 = []
        if self.lang == "ja":
            # extension_rank_1 = hiragana + katakana + katakana_chouon + youon + sokuon_hiragana + sokuon_katakana + gairaigo_katakana + alphabet + number + kanji
            extension_rank_1 = hiragana + katakana + katakana_chouon + alphabet + number + kanji

        print(f"[{datetime.now()}] 1단계 확장 텍스트 개수 : {len(extension_rank_1)}")
        return extension_rank_1
    
    @error_notifier
    def get_2st_extension(self) -> List[str]:
        '''
        2단계 확장 텍스트 가져오기
        '''
        extension_rank_2 = []
        if self.lang == "ja":
            # extension_rank_2 = hiragana + katakana + katakana_chouon + youon + sokuon_hiragana + sokuon_katakana + gairaigo_katakana + alphabet + number
            extension_rank_2 = hiragana + katakana + katakana_chouon + alphabet + number

        print(f"[{datetime.now()}] 2단계 확장 텍스트 개수 : {len(extension_rank_2)}")
        return extension_rank_2
    
    @error_notifier
    def get_3st_extension(self) -> List[str]:
        '''
        3단계 확장 텍스트 가져오기
        '''
        extension_rank_3 = []
        if self.lang == "ja":
            # extension_rank_3 = hiragana + katakana + katakana_chouon + youon + sokuon_hiragana + sokuon_katakana + gairaigo_katakana + alphabet + number
            extension_rank_3 = hiragana + katakana + katakana_chouon + alphabet + number

        print(f"[{datetime.now()}] 3단계 확장 텍스트 개수 : {len(extension_rank_3)}")
        return extension_rank_3
    
    @error_notifier
    def filtering_valid_trend_keywords(self,
                                       targets:List[str],
                                       valid_threshold:int) -> List[str]:
        '''
        유효한 트렌드 키워드 추출
        '''
        valid_targets = []
        start_time = datetime.now()
        for line in JsonlFileHandler(self.local_result_path).read_generator():
            keyword = line['keyword']
            if keyword in targets:
                valid_suggest_cnt, valid_suggests = cnt_valid_suggest(suggestions=line['suggestions'],
                                                                      input_text=keyword,
                                                                      return_result=True)
                if valid_suggest_cnt >= valid_threshold:
                    valid_targets.append(keyword)
        print(f"[{datetime.now()}] 유효한 키워드 추출 소요 시간 : {datetime.now()-start_time}")
        return valid_targets
        
    @error_notifier
    def run_basic(self):
        '''
        기본 서제스트 수집
        '''
        basic_num_process = 100
        print(f"[{datetime.now()}] job_id : {self.job_id} | service : {self.service} ⭐기본 서제스트⭐ 수집 시작")

        # 1단계 확장 텍스트 가져오기
        print(f"[{datetime.now()}] 📍 1단계")
        targets_1 = self.get_1st_extension()
        # 서제스트 수집
        print(f"[{datetime.now()}] ✅ 1단계 수집 시작 (총 수집할 개수 : {len(targets_1)}, process_num : {basic_num_process})")
        targets = targets_1
        self.statistics["call"]["rank1"] = len(targets)
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=basic_num_process)

        valid_targets_1 = self.filtering_valid_trend_keywords(targets_1, 6)
        print(f"✅ 1단계 유효한 키워드 개수 : {len(valid_targets_1)}/{len(targets_1)}")

        # 2단계 확장 텍스트 가져오기
        print(f"[{datetime.now()}] 📍 2단계")
        extension_2 = self.get_2st_extension()
        targets_2 = [x + y for x in valid_targets_1 for y in extension_2]
        # 서제스트 수집
        print(f"[{datetime.now()}] ✅ 2단계 수집 시작 (총 수집할 개수 : {len(targets_2)}, process_num : {basic_num_process})")
        already_collected_keywords = self.get_already_collected_keywords() # 이미 수집한 키워드 목록
        targets = list(set(targets_2) - set(already_collected_keywords))
        self.statistics["call"]["rank2"] = len(targets)
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=basic_num_process)

        # 2단계 확장 텍스트 가져오기
        valid_targets_2 = self.filtering_valid_trend_keywords(targets_2, 6)
        print(f"✅ 2단계 유효한 키워드 개수 : {len(valid_targets_2)}/{len(targets_2)}")

        # 3단계 확장 텍스트 가져오기
        print(f"[{datetime.now()}] 📍 3단계")
        extension_3 = self.get_3st_extension()
        targets_3 = [x + y for x in valid_targets_2 for y in extension_3]
        # 서제스트 수집
        print(f"[{datetime.now()}] ✅ 3단계 수집 시작 (총 수집할 개수 : {len(targets_3)}, process_num : {basic_num_process})")
        already_collected_keywords = self.get_already_collected_keywords() # 이미 수집한 키워드 목록
        targets = list(set(targets_3) - set(already_collected_keywords))
        self.statistics["call"]["rank3"] = len(targets)
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=basic_num_process)

        self.local_result_path = GZipFileHandler.gzip(self.local_result_path)
        print(f"[{datetime.now()}] 🎉 기본 서제스트 수집 완료")

    @error_notifier
    def count_trend_keyword(self) -> dict:
        # 총 트렌드 키워드 개수
        trend_keywords = TXTFileHandler(self.trend_keyword_file).read_lines()
        trend_keywords = remove_duplicates_with_spaces(trend_keywords)
        print(f"[{datetime.now()}] {self.lang} {self.service} 트렌드 키워드 개수 : {len(trend_keywords)}")
        # 새로 나온 트렌드 키워드 개수
        new_trend_keywords = TXTFileHandler(self.new_trend_keyword_file).read_lines()
        new_trend_keywords = remove_duplicates_with_spaces(new_trend_keywords)
        print(f"[{datetime.now()}] {self.lang} {self.service} 새로 나온 트렌드 키워드 개수 : {len(new_trend_keywords)}")
        return {"total": len(trend_keywords),
                "new": len(new_trend_keywords)}

    @error_notifier
    def upload_to_hdfs(self):
        basic_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}.jsonl.gz"
        self.hdfs.upload(source=self.local_result_path, dest=basic_hdfs_path, overwrite=True)

        trend_keyword_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}_trend_keywords.txt"
        self.hdfs.upload(source=self.trend_keyword_file, dest=trend_keyword_hdfs_path, overwrite=True)

        new_trend_keyword_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}_trend_keywords_new.txt"
        self.hdfs.upload(source=self.new_trend_keyword_file, dest=new_trend_keyword_hdfs_path, overwrite=True)

    def run(self):
        try:
            start_time = datetime.now()
            if self.log_task_history:
                self.task_history_updater.set_task_start()
                self.task_history_updater.set_task_in_progress()

            self.run_basic()

            trend_keyword_cnt = self.count_trend_keyword()
            self.statistics["trend_keyword"] = trend_keyword_cnt

            self.upload_to_hdfs()
            
            if self.log_task_history:
                self.task_history_updater.set_task_completed(additional_info=self.statistics)
            end_time = datetime.now()
        except Exception as e:
            print(f"[{datetime.now()}] 서제스트 수집 실패 작업 종료\nError Msg : {e}")
            error_msg = (
                f"{self.slack_prefix_msg}\n"
                f"Message: 서제스트 수집 실패 작업 종료\n"
                f"Error: {e}"
            )
            ds_trend_finder_dbgout_error(self.lang, error_msg)
            if self.log_task_history:
                self.task_history_updater.set_task_error(error_msg=str(e))
        else:
            print(f"[{datetime.now()}] 서제스트 수집 완료")
            print(f"[Statistics]\n{self.statistics}")
            success_msg = (
                            f"{self.slack_prefix_msg}\n"
                            f"Message: 서제스트 수집 완료\n"
                            f"Upload Path: {self.hdfs_upload_folder}\n"
                            f"Processing Time: {end_time-start_time} 소요\n"
                            f"Statistics: (total: {trend_keyword_cnt['total']} | new: {trend_keyword_cnt['new']})\n"
                        )
            ds_trend_finder_dbgout(self.lang, success_msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", help="language", default=None)
    parser.add_argument("--service", help="service(google or youtube)", default=None)
    args = parser.parse_args()
    
    pid = os.getgid()
    print(f"pid : {pid}")

    # job_id 생성
    if args.lang == "en": # 미국일 경우 현재 시간에서 14시간 이전으로 job_id 조정
        job_id = adjust_job_id(datetime.now().strftime("%Y%m%d%H"), 14)
    else:
        job_id = datetime.now().strftime("%Y%m%d%H")

    print(f"job_id : {job_id}")

    # 수집 시작
    print(f"---------- [{datetime.now()}] {args.lang} {args.service} 수집 시작 ----------")
    entity_daily = EntitySuggestDaily(args.lang, args.service, job_id, log_task_history=True)
    entity_daily.run()
    print(f"---------- [{datetime.now()}] {args.lang} {args.service} 수집 완료 ----------")