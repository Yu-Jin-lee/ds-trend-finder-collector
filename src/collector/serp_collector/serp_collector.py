# 1. 키워드 리스트 받은 뒤 서프 수집 카프카에게 요청
# 2. 서프 모두 수집되었는지 확인
from typing import List, Union
import time
from datetime import datetime
import requests, json
from http import HTTPStatus
from dataclasses import dataclass, field, asdict
from multiprocessing import Pool

from validator.serp_validator import SerpValidator
from utils.kafka import request_collect_serp_to_kafka
from utils.function import current_function_name
from utils.db import QueryDatabaseKo, QueryDatabaseJa, QueryDatabaseEn
from utils.file import JsonlFileHandler
from lang import Ko, Ja, En

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

@dataclass
class SerpApiParams:
    q:str= ''
    device:str= 'mobile'
    location:str= "South Korea"
    hl:str= "ko"
    gl:str= "kr"
    usage_id:str= "yujin.lee"
    meta:dict= field(default_factory=dict)
    num:int= 20
    
class SerpCollector:
    SERP_API_URL = "http://google-serp-api.ascentlab.io/serpapi/search"
    SERP_HEADERS = {"Content-Type": "application/json; charset=UTF-8"}

    def __init__(self,
                 lang:str):
        self.db = self.set_database(lang)
        self.hl, self.gl, self.serp_location = self.set_lang_info(lang)
    
    def set_lang_info(self, lang):
        if lang == 'ko':
            return Ko().hl, Ko().gl, Ko().serp_location()
        if lang == 'ja': 
            return Ja().hl, Ja().gl, Ja().serp_location()
        if lang == "en":
            return En().hl, En().gl, En().serp_location()
        
    def set_database(self, lang):
        if lang == "ko":
            return QueryDatabaseKo()
        if lang == "ja":
            return QueryDatabaseJa()
        if lang == "en":
            return QueryDatabaseEn()
        
    def send_to_kafka(self, keywords : List[str]):
        '''
        키워드 리스트 받은 뒤 서프 수집 카프카에게 요청
        '''
        try:
            for keyword in keywords:
                request_collect_serp_to_kafka(keyword)
        except Exception as e:
            print(f"[{datetime.now()}] error from {current_function_name()} : {e}")
        else:
            print(f"[{datetime.now()}] 서프 수집을 위한 카프카 요청 완료 : {len(keywords)}개 키워드")
            
    def check_all_keywords_collected(self, 
                                     keywords : List[str],
                                     return_result : bool = False,
                                     collected_time : str = None # yyyy-mm-dd hh:mm:ss # 특정 날짜 이후의 서프만 확인
                                     ) -> Union["pd.DataFrame", bool]:
        '''
        서프 모두 수집되었는지 확인 (serp_history 테이블에서 조회하여 확인)
        '''
        print(f"[{datetime.now()}] 서프 수집 완료 확인 시작")
        keywords = [k.lower() for k in keywords] # 대소문자 구분 안함 (디비에는 구분 안해 놓음)
        if collected_time == None:
            serp_hash_json_df = self.db.get_hash_json_by_keywords(keywords) # serp_history 테이블에서 조회
        else:
            serp_hash_json_df = self.db.get_hash_json_over_collected_time(keywords, collected_time)

        wait_time = 60*5
        while set(keywords) != set(serp_hash_json_df['keyword']):
            print(f"[{datetime.now()}] {len(set(serp_hash_json_df['keyword']))}/{len(set(keywords))}개 수집 완료")
            not_collected_keywords = set(keywords) - set(serp_hash_json_df['keyword'])
            if len(not_collected_keywords) <= 200:
                self.send_to_kafka(not_collected_keywords)
                wait_time = 60*7
            time.sleep(wait_time) # 5분 기다림
            serp_hash_json_df = self.db.get_hash_json_by_keywords(keywords) # serp_history 테이블에서 조회
        
        print(f"[{datetime.now()}] 모든 서프 수집 완료 : {len(keywords)}개 키워드")
        if return_result:
            return serp_hash_json_df
        else:
            return True
    
    def send_to_kafka_and_check_all_keywords_collected(self, 
                                                       keywords : List[str],
                                                       return_result : bool = False, # 결과 리턴 할건지 여부 (True : 키워드, hash, json 리턴 / False : None 리턴)
                                                       collected_time : str = None # yyyy-mm-dd hh:mm:ss # 특정 날짜 이후의 서프만 확인
                                                       ):
        '''
        카프카에 수집 요청 후 모든 서프 수집 완료 확인 (대소문자 구분 안한 결과가 반환됨, serp_history 테이블에 대소문자 구분 안되어 있음)
        '''
        print(f"[{datetime.now()}] 카프카에 서프 수집 요청 시작")
        self.send_to_kafka(keywords)
        res = self.check_all_keywords_collected(keywords, return_result=return_result, collected_time=collected_time)
        return res
    
    def get_serp_by_hash(self, hash_value):
        url = f"https://mongttang-data-api.ascentlab.io/serp/?row_key={hash_value}&f=json"
        response = requests.get(url)
        serp = json.loads(response.text)
    
        return serp

    def get_serp_by_json(self, json_value):
        response = requests.get(json_value)
        serp = json.loads(response.text)
        
        return serp
    
    def get_serp_by_json_or_hash(self, json_value, hash_value) -> Union[dict, None]:
        serp = self.get_serp_by_hash(hash_value)
        if not SerpValidator(serp).validate():
            serp = self.get_serp_by_json(json_value)
            if SerpValidator(serp).validate():
                return serp
            else:
                print(f"[{datetime.now()}] {hash_value}와 {json_value}로 서프 조회 실패")
                return None
        else:
            return serp
                   
    def get_serps(self, keywords : List[str]) -> dict:
        '''
        input으로 받은 키워드 리스트의 모든 서프 반환
        '''
        serp_hash_json_df = self.db.get_hash_json_by_keywords(keywords) # serp_history 테이블에서 조회
        serps_by_keyword = {}
        if len(serp_hash_json_df)> 0:
            for i in range(len(serp_hash_json_df)):
                keyword = serp_hash_json_df.loc[i, 'keyword']
                hash_value = serp_hash_json_df.loc[i, 'hash']
                json_value = serp_hash_json_df.loc[i, 'json']
                serp = self.get_serp_by_hash(hash_value)
                if not SerpValidator(serp).validate():
                    serp = self.get_serp_by_json(json_value)
                    if SerpValidator(serp).validate():
                        serps_by_keyword[keyword] = serp
                else:
                   serps_by_keyword[keyword] = serp            
            
            return serps_by_keyword
        
    def get_serps_over_collected_time(self, 
                                      keywords : List[str], 
                                      collected_time : str # yyyy-mm-dd hh:mm:ss
                                      ) -> dict:
        '''
        input으로 받은 키워드 리스트 중 입력된 collected_time 이후에 수집된 서프만 반환
        '''
        serp_hash_json_df = self.db.get_hash_json_over_collected_time(keywords, collected_time) # serp_history 테이블에서 조회
        print(f"[{datetime.now()}] {len(serp_hash_json_df)}/{len(keywords)}개 키워드 {collected_time} 이후 수집된 서프 존재")
        serps_by_keyword = {}
        if len(serp_hash_json_df)> 0:
            for i in range(len(serp_hash_json_df)):
                keyword = serp_hash_json_df.loc[i, 'keyword']
                hash_value = serp_hash_json_df.loc[i, 'hash']
                json_value = serp_hash_json_df.loc[i, 'json']
                serp = self.get_serp_by_hash(hash_value)
                if not SerpValidator(serp).validate():
                    serp = self.get_serp_by_json(json_value)
                    if SerpValidator(serp).validate():
                        serps_by_keyword[keyword] = serp
                else:
                   serps_by_keyword[keyword] = serp            
            
            return serps_by_keyword
        
    def save_serp_to_local(self,
                           keywords : List[str],
                           save_path : str, # *.jsonl 형태로 저장할 경로
                           collected_time : str = None # yyyy-mm-dd hh:mm:ss
                           ) -> str: # 저장된 경로 반환
        '''
        save_path 에 jsonl 형태로 서프 저장
        '''
        if collected_time == None:
            json_hash = self.db.get_hash_json_by_keywords(keywords)
        else:
            json_hash = self.db.get_hash_json_over_collected_time(keywords, collected_time)
        print(f"[{datetime.now()}] {len(json_hash)}/{len(keywords)}개 서프 저장 시작 (save_path : {save_path})")
        success_cnt = 0
        for i in range(len(json_hash)):
            keyword = json_hash.loc[i, 'keyword']
            hash_value = json_hash.loc[i, 'hash']
            json_value = json_hash.loc[i, 'json']
            serp = self.get_serp_by_json_or_hash(json_value, hash_value)
            if serp != None:
                success_cnt += 1
                JsonlFileHandler(save_path).write(serp)
        
        print(f"[{datetime.now()}] {success_cnt}/{len(keywords)}개 서프 저장 완료 (save_path : {save_path})")
        
        return save_path
    
    #### hdfs에서 서프 가져오기 ####
    def get_recent_serp_rowkey(self, 
                               query: str
                              ):
        '''
        ex)  
            serp_collector_ko = SerpCollector('ko')
            serp = serp_collector_ko.get_serp_by_rowkey(serp_collector_ko.get_recent_serp_rowkey('국민은행'))
        '''
        url = "http://mongttang-data-api.ascentlab.io/serp/list"
        params = {
            "q": query,
            "hl": self.hl,
            "gl": self.gl,
            "engine": "GG",
            "device": "M",
            "reserve": True,
            "limit": 1,
        }
        res = requests.get(url, params=params)
        if res.status_code == HTTPStatus.OK:
            rowkeys = list(res.json())
        else:
            rowkeys = []
        return list(rowkeys)[0]
    
    def get_serp_by_rowkey(self, rowkey: str = "",f: str = "json"):
        '''
        ex) 
            serp_collector_ko = SerpCollector('ko')
            serp = serp_collector_ko.get_serp_by_rowkey(serp_collector_ko.get_recent_serp_rowkey('국민은행'))
        '''
        f = "json" if f == "json" else "html"
        url = "http://mongttang-data-api.ascentlab.io/serp/"
        params = {"row_key": rowkey, "f": f}
        res = requests.get(url, params=params)
        return res.json()
    ####################################
    
    def fetch_serp_data(self, serp_api_params):
        response = requests.post(SerpCollector.SERP_API_URL,
                                 headers=SerpCollector.SERP_HEADERS,
                                 json=asdict(serp_api_params),
                                 timeout=60.0 * 5 + 15.0)
        if response.status_code == HTTPStatus.OK:
            _serp = json.loads(response.text)
            print(f"get_serp_from_serp_api {serp_api_params.q}")
            return _serp, []
        else:
            print(f'[{serp_api_params.q}] - {response.status_code} error')
            return [], [{'query': serp_api_params.q, 'cause': f'{response.status_code}'}]
        
    def get_serp_from_serp_api(self, 
                               keywords : List[str],
                               domain : str = "suggest_issue",
                               num_processes : int = 30):
        '''
        serp_api를 통해 서프 수집
        '''
        print(f"[{datetime.now()}] serp_api를 통한 서프 수집 시작 (총 {len(keywords)}개 키워드)")
        serp_api_paramses:List[SerpApiParams]=[]
        for k in keywords:
            serp_api_paramses.append(SerpApiParams(q = k,
                                                    hl = self.hl,
                                                    gl = self.gl,
                                                    location = self.serp_location,
                                                    meta = {"seed_keyword": None, 
                                                            "domain": domain,
                                                            "purpose": "adhoc"}))

        with Pool(num_processes) as pool:
            results = pool.map(self.fetch_serp_data, serp_api_paramses)

        results, error_results = zip(*results)

        results = [res for res in results if res != []]
        error_results = [res for res in error_results if res != []]

        print(f"[{datetime.now()}] serp_api를 통한 서프 수집 완료 (성공 : {len(results)}개 / 실패 : {len(error_results)}개)")
        return list(results), list(error_results)

    
        
if __name__ == "__main__":
    keyword = 'bb lab 저분자 콜라겐'
    serp_collector = SerpCollector("ko")
    res, error_res = serp_collector.get_serp_from_serp_api([keyword])
    print(res)
    