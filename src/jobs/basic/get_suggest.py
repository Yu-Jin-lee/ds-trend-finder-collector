import os
import gc
import math
import argparse
from datetime import datetime, timedelta
from typing import List

from collector.suggest_collector.suggest_collect import Suggest
from validator.trend_keyword_validator import is_trend_keyword, cnt_valid_suggest
from utils.file import JsonlFileHandler, GZipFileHandler, TXTFileHandler, has_file_extension
from utils.data import Trie, remove_duplicates_from_new_keywords
from utils.hdfs import HdfsFileHandler
from utils.task_history import TaskHistory
from utils.slack import ds_trend_finder_dbgout, ds_trend_finder_dbgout_error
from utils.decorator import error_notifier
from lang import Ko, Ja, En, filter_en_valid_trend_keyword, filter_en_valid_token_count
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
        self.one_week_ago_trend_keywords : List[str] = self.get_one_week_ago_trend_keywords(self.job_id[:8], lang) # 이전 7일간 나왔던 트렌드 키워드
        
        # Task History 관련
        self.task_name = f"수집-서제스트-{service}-{self.suggest_type}"
        self.log_task_history = log_task_history
        self.task_history_updater = TaskHistory(postgres_db_config, self.project_name, self.task_name, self.job_id, self.lang)

        # slack 관련
        self.slack_prefix_msg = f"Job Id : `{self.job_id}`\nTask Name : `{self.task_name}`-`{self.lang}`"

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
    def get_one_week_ago_trend_keywords(self, today, lang) -> List[str]:
        '''
        이전 7일 트렌드 키워드 목록 가져오기
        '''
        services = ['google', 'youtube']
        
        today_datetime = datetime.strptime(today, "%Y%m%d")

        one_week_ago_trend_keywords = []
        for i in range(1, 8, 1):
            date = today_datetime - timedelta(days=i)
            date = date.strftime("%Y%m%d")
            for service in services:
                txt_files = self.get_all_txt_files(f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_llm_entity_topic/{date[:4]}/{date[:6]}/{date[:8]}")
                for file in txt_files:
                    one_week_ago_trend_keywords += self.load_keywords_from_hdfs(file)

        one_week_ago_trend_keywords = list(set(one_week_ago_trend_keywords))
        print(f"키워드 개수 : {len(set(one_week_ago_trend_keywords))}")

        return one_week_ago_trend_keywords
    
    @error_notifier
    def get_basic_extension(self) -> List[str]:
        '''
        대상 키워드 없을 경우 확장 텍스트 가져오기
        '''
        lang = self.get_lang(self.lang)
        if self.lang == "ko":
            complete_letters = ['가', '개', '거', '게', '겨', '계', '고', '과', '괴', '교', '구', '궈', '궤', '귀', '규', '그', '기', '까', '깨', '꺼', '께', '껴', '꼬', '꽤', '꾀', '꾸', '꿔', '꿰', '뀌', '끄', '끼', '나', '내', '냐', '너', '네', '녀', '노', '놔', '뇌', '뇨', '누', '눠', '뉘', '뉴', '느', '늬', '니', '다', '대', '더', '데', '뎌', '도', '돼', '되', '두', '둬', '뒤', '듀', '드', '디', '따', '때', '떠', '떼', '또', '뚜', '뛰', '뜨', '띄', '띠', '라', '래', '랴', '러', '레', '려', '례', '로', '뢰', '료', '루', '뤄', '뤼', '류', '르', '리', '마', '매', '머', '메', '며', '모', '묘', '무', '뭐', '뮤', '므', '미', '바', '배', '버', '베', '벼', '보', '봐', '부', '뷔', '뷰', '브', '비', '빠', '빼', '뻐', '뼈', '뽀', '뾰', '뿌', '쁘', '삐', '사', '새', '샤', '섀', '서', '세', '셔', '셰', '소', '쇄', '쇠', '쇼', '수', '쉐', '쉬', '슈', '스', '시', '싸', '써', '쎄', '쏘', '쐐', '쑤', '쓰', '씌', '씨', '아', '애', '야', '얘', '어', '에', '여', '예', '오', '와', '왜', '외', '요', '우', '워', '웨', '위', '유', '으', '의', '이', '자', '재', '저', '제', '져', '조', '좌', '죄', '죠', '주', '줘', '쥐', '쥬', '즈', '지', '짜', '째', '쩌', '쪼', '쯔', '찌', '차', '채', '처', '체', '쳐', '초', '최', '추', '춰', '췌', '취', '츄', '츠', '치', '카', '캐', '커', '케', '켜', '코', '콰', '쾌', '쿄', '쿠', '쿼', '퀘', '퀴', '큐', '크', '키', '타', '태', '터', '테', '텨', '토', '퇴', '투', '퉈', '튀', '튜', '트', '티', '파', '패', '퍼', '페', '펴', '폐', '포', '표', '푸', '퓨', '프', '피', '하', '해', '허', '헤', '혀', '혜', '호', '화', '회', '효', '후', '훼', '휘', '휴', '흐', '희', '히']
            suggest_extension_texts_rank_4 = [x + y for x in complete_letters for y in complete_letters]
            return lang.suggest_extension_texts_by_rank(1) + lang.suggest_extension_texts_by_rank(2) + lang.suggest_extension_texts_by_rank(3) + suggest_extension_texts_rank_4
        if self.lang == "ja": # 일본
            return lang.suggest_extension_texts_by_rank("1_kanji_300") + lang.suggest_extension_texts_by_rank("2_kanji_300") + lang.suggest_extension_texts_by_rank("3_kanji_300")
        if self.lang == "en":
            return lang.suggest_extension_texts_by_rank(1) + lang.suggest_extension_texts_by_rank(2) + lang.suggest_extension_texts_by_rank(3) + lang.suggest_extension_texts_by_rank(4)
        else:
            print(f"[{datetime.now()}] {self.lang} 해당 국가는 지원하지 않습니다. (EntitySuggestDaily-get_basic_extension)")
            raise ValueError(f"{self.lang} 해당 국가는 지원하지 않습니다. (EntitySuggestDaily-get_basic_extension)")
        
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
                new_trend_keywords = list(remove_duplicates_from_new_keywords(set(self.one_week_ago_trend_keywords), set(valid_trend_keywords)))
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
    def run_basic(self):
        '''
        기본 서제스트 수집
        '''
        basic_num_process = 100
        print(f"[{datetime.now()}] job_id : {self.job_id} | service : {self.service} ⭐기본 서제스트⭐ 수집 시작")

        # 기본 서제스트
        basic_extension = self.get_basic_extension()
        targets = basic_extension # 서제스트 수집할 키워드 리스트
        print(f"[{datetime.now()}] 기본 extension text 추가 후 개수 {len(targets)}")

        # 기본 서제스트 수집
        print(f"[{datetime.now()}] 기본 서제스트 수집 시작 (총 수집할 개수 : {len(targets)}, process_num : {basic_num_process})")
        
        already_collected_keywords = self.get_already_collected_keywords()
        targets = list(set(targets) - set(already_collected_keywords))
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=basic_num_process)
        self.local_result_path = GZipFileHandler.gzip(self.local_result_path)
        print(f"[{datetime.now()}] 기본 서제스트 수집 완료")

    @error_notifier
    def filtering_rank_4_targets(self) -> List[str]:
        '''
        4단계 수집할 target 추출
        2단계에서 valid한 서제스트가 valid_threshold개 이상인 완성형 문자로 시작하는 확장 문자만 수집

        수집한 파일에서 2단계에 해당하는 text 판별
        2단계에서 valid한 서제스트가 valid_threshold개 이상인 완성형 문자로 시작하는 확장 문자만 수집
        '''
        lang = self.get_lang(self.lang)
        rank2_targets = lang.suggest_extension_texts_by_rank("2_small")
        rank4_targets = lang.suggest_extension_texts_by_rank("4_small")
        
        print(f"\n[{datetime.now()}] 4단계 수집할 target 추출 시작")
        targets = []
        valid_threshold = 6 # 최소 valid한 서제스트 개수
        print(f"[{datetime.now()}] valid_threshold : {valid_threshold}")
        valid_suggest_result_path = f"{self.local_folder_path}/{self.job_id}_valid_suggests_rank4_thres{valid_threshold}.jsonl"
        for line in JsonlFileHandler(self.local_result_path).read_generator():
            keyword = line['keyword']
            if keyword in rank2_targets: # 2단계 확장 문자로 수집한 결과일 경우
                valid_suggest_cnt, valid_suggests = cnt_valid_suggest(suggestions=line['suggestions'], 
                                                                      input_text=keyword, 
                                                                      return_result=True)
                JsonlFileHandler(valid_suggest_result_path).write({keyword : valid_suggests})
                if valid_suggest_cnt >= valid_threshold:
                    targets += [t for t in rank4_targets if t.startswith(keyword)] # 4단계 확장 문자 중 2단계 확장 문자로 시작하는 것만 추출
        targets = list(set(targets))
        print(f"[{datetime.now()}] 4단계 수집할 target 추출 완료\n")

        print(f"[{datetime.now()}] 4단계 수집할 target 개수 : {len(targets)}개/{len(rank4_targets)}개중")
        GZipFileHandler.gzip(valid_suggest_result_path)
        TXTFileHandler(f"{self.local_folder_path}/{self.job_id}_target_rank4_thres{valid_threshold}.txt").write(targets)
        return targets

    @error_notifier
    def filtering_rank_5_targets(self) -> List[str]:
        '''
        5단계 수집할 target 추출
        4단계에서 valid한 서제스트가 valid_threshold개 이상인 완성형 문자로 시작하는 확장 문자만 수집
        '''
        lang = self.get_lang(self.lang)
        rank4_targets = lang.suggest_extension_texts_by_rank("4_small")
        rank5_targets = lang.suggest_extension_texts_by_rank("5_small")
        
        print(f"\n[{datetime.now()}] Trie 생성 시작")
        trie = Trie()
        for target in rank5_targets:
            trie.insert(target)
        print(f"[{datetime.now()}] Trie 생성 완료\n")

        print(f"\n[{datetime.now()}] 5단계 수집할 target 추출 시작")
        targets = []
        valid_threshold = 6 # 최소 valid한 서제스트 개수
        print(f"[{datetime.now()}] valid_threshold : {valid_threshold}")
        valid_suggest_result_path = f"{self.local_folder_path}/{self.job_id}_valid_suggests_rank5_thres{valid_threshold}.jsonl"
        for line in JsonlFileHandler(self.local_result_path).read_generator():
            keyword = line['keyword']
            if keyword in rank4_targets: # 4단계 확장 문자로 수집한 결과일 경우
                valid_suggest_cnt, valid_suggests = cnt_valid_suggest(suggestions=line['suggestions'], 
                                                                      input_text=keyword, 
                                                                      return_result=True)
                JsonlFileHandler(valid_suggest_result_path).write({keyword : valid_suggests})
                if valid_suggest_cnt >= valid_threshold:
                    filtered_targets = trie.starts_with(keyword)
                    targets.extend(filtered_targets) # 5단계 확장 문자 중 4단계 확장 문자로 시작하는 것만 추출
        targets = list(set(targets))
        print(f"[{datetime.now()}] 5단계 수집할 target 추출 완료\n")

        print(f"[{datetime.now()}] 5단계 수집할 target 개수 : {len(targets)}개/{len(rank5_targets)}개중")

        GZipFileHandler.gzip(valid_suggest_result_path)
        TXTFileHandler(f"{self.local_folder_path}/{self.job_id}_target_rank5_thres{valid_threshold}.txt").write(targets)

        del trie
        gc.collect()
        return targets
    
    @error_notifier
    def run_basic_ko(self):
        '''
        한국 기본 서제스트 수집
        '''
        print(f"\n\n[{datetime.now()}] {self.service} {self.lang} 기본 서제스트 수집 시작")
        lang = self.get_lang(self.lang)
        num_process = 100
        # 1, 2, 3 단계 모두 수집
        targets = lang.suggest_extension_texts_by_rank(1) + \
                lang.suggest_extension_texts_by_rank("2_small") + \
                lang.suggest_extension_texts_by_rank("3_small")
        print(f"[{datetime.now()}] 1, 2, 3 단계 extension text 추가 후 개수 {len(targets)} | process_num : {num_process}")
        already_collected_keywords = self.get_already_collected_keywords()
        targets = list(set(targets) - set(already_collected_keywords))
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=num_process)

        # 4단계 수집
        ## 2단계에서 valid한 서제스트가 valid_threshold개 이상인 완성형 문자로 시작하는 확장 문자만 수집
        targets = self.filtering_rank_4_targets()
        already_collected_keywords = self.get_already_collected_keywords()
        targets = list(set(targets) - set(already_collected_keywords))
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=num_process)

        # 5단계 수집
        ## 4단계에서 valid한 서제스트가 valid_threshold개 이상인 완성형 문자로 시작하는 확장 문자만 수집
        targets = self.filtering_rank_5_targets()
        already_collected_keywords = self.get_already_collected_keywords()
        targets = list(set(targets) - set(already_collected_keywords))
        self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=num_process)

        self.local_result_path = GZipFileHandler.gzip(self.local_result_path)
        print(f"[{datetime.now()}] {self.service} {self.lang} 기본 서제스트 수집 완료\n\n")

    @error_notifier
    def count_trend_keyword(self) -> int:
        trend_keywords = TXTFileHandler(self.trend_keyword_file).read_lines()
        trend_keywords = list(set(trend_keywords))
        print(f"[{datetime.now()}] {self.lang} {self.service} 트렌드 키워드 개수 : {len(trend_keywords)}")
        return len(trend_keywords)

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

            if self.lang == "ko":
                self.run_basic_ko()
            else:
                self.run_basic()

            self.count_trend_keyword()

            self.upload_to_hdfs()
            
            if self.log_task_history:
                self.task_history_updater.set_task_completed()
            end_time = datetime.now()
        except Exception as e:
            print(f"[{datetime.now()}] 서제스트 수집 실패 작업 종료\nError Msg : {e}")
            ds_trend_finder_dbgout_error(f"{self.slack_prefix_msg}\nMessage : 서제스트 수집 실패 작업 종료")
            if self.log_task_history:
                self.task_history_updater.set_task_error(error_msg=e)
        else:
            print(f"[{datetime.now()}] 서제스트 수집 완료")
            ds_trend_finder_dbgout(f"{self.slack_prefix_msg}\nMessage : 서제스트 수집 완료\nUpload Path : {self.hdfs_upload_folder}\n{end_time-start_time} 소요")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", help="language", default=None)
    parser.add_argument("--service", help="service(google or youtube)", default=None)
    args = parser.parse_args()
    
    pid = os.getgid()
    print(f"pid : {pid}")

    print(f"---------- [{datetime.now()}] {args.lang} {args.service} 수집 시작 ----------")
    entity_daily = EntitySuggestDaily(args.lang, args.service, datetime.now().strftime("%Y%m%d%H"), log_task_history=True)
    entity_daily.run()
    print(f"---------- [{datetime.now()}] {args.lang} {args.service} 수집 완료 ----------")