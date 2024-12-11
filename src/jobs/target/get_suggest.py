import os
import math
import argparse
from datetime import datetime, timedelta
from typing import List, Tuple

from collector.suggest_collector.suggest_collect import Suggest
from validator.trend_keyword_validator import is_trend_keyword
from utils.file import JsonlFileHandler, GZipFileHandler, TXTFileHandler, JsonFileHandler, has_file_extension
from utils.db import QueryDatabaseKo, QueryDatabaseJa, QueryDatabaseEn
from utils.text import extract_initial
from utils.data import combine_dictionary
from utils.hdfs import HdfsFileHandler
from utils.postgres import PostGres
from lang import Ko, Ja, En, filter_en_valid_trend_keyword, filter_en_valid_token_count
from config import postgres_db_config
from utils.decorator import error_notifier
from utils.task_history import TaskHistory
from utils.data import remove_duplicates_from_new_keywords
from validator.suggest_validator import SuggestValidator
from utils.text import extract_initial_next_target_keyword
from utils.converter import adjust_job_id
from utils.slack import ds_trend_finder_dbgout, ds_trend_finder_dbgout_error

def cnt_valid_suggest(suggestions:List[dict], 
                      target_keyword:str=None,
                      extension:str=None, # 알파벳 확장 문자 (있을 경우 입력, 없을 경우:None)
                      log:bool=False,
                      return_result:bool=False) -> int:
    try:
        cnt_valid = 0
        valid_suggest = []
        for suggestion in suggestions:
            if SuggestValidator.is_valid_suggest(suggestion['suggest_type'], suggestion['suggest_subtypes']):
                if (target_keyword != None and
                    extension != None): # 타겟 키워드와 확장 문자가 모두 있을 경우
                    initial_next_target_keyword = extract_initial_next_target_keyword([suggestion['text']], target_keyword=target_keyword)
                    if initial_next_target_keyword and len(initial_next_target_keyword) > 0:
                        if initial_next_target_keyword[0] == extension:
                            if log:
                                print(f"✔️ {suggestion['text']} {suggestion['suggest_type']} {suggestion['suggest_subtypes']}")
                            cnt_valid += 1
                            valid_suggest.append(suggestion['text'])
                        else:
                            if log:
                                print(f"❌❗ {suggestion['text']} {suggestion['suggest_type']} {suggestion['suggest_subtypes']}")
                    else:
                        if log:
                            print(f"❌❗ {suggestion['text']} {suggestion['suggest_type']} {suggestion['suggest_subtypes']}")
                else:
                    if log:
                        print(f"✔️ {suggestion['text']} {suggestion['suggest_type']} {suggestion['suggest_subtypes']}")
                    cnt_valid += 1
                    valid_suggest.append(suggestion['text'])
            else:
                if log:
                    print(f"❌ {suggestion['text']} {suggestion['suggest_type']} {suggestion['suggest_subtypes']}")
        if return_result:
            return cnt_valid, valid_suggest
        return cnt_valid
    except Exception as e:
        if log:
            print(f"[{datetime.now()}] Error from cnt_valid_suggest: (target_keyword:{target_keyword}, extension:{extension}) | error msg : {e}")
        if return_result:
            return cnt_valid, valid_suggest
        return cnt_valid

class EntitySuggestDaily:
    def __init__(self, lang : str, service : str, job_id, log_task_history:bool=False):
        # 기본정보
        self.lang = lang
        self.service = service
        self.job_id = job_id
        self.project_name = "trend_finder"
        self.suggest_type = "target"
        self.task_name = f"수집-서제스트-{service}-{self.suggest_type}"
        self.target_letter_suggest_length = None

        # local 관련
        self.local_folder_path = f"./data/result/{self.suggest_type}/{self.service}/{self.lang}"
        if not os.path.exists(self.local_folder_path):
            os.makedirs(self.local_folder_path)
        self.trend_keyword_file = f"{self.local_folder_path}/{self.job_id}_trend_keywords.txt"
        self.new_trend_keyword_file = f"{self.local_folder_path}/{self.job_id}_trend_keywords_new.txt" # 새로 나온 트렌드 키워드 저장
        self.except_for_valid_trend_keywords_file = f"{self.local_folder_path}/{self.job_id}_except_for_valid_trend_keywords.txt" # 유효하지 않은 트렌드 키워드 저장
        self.local_result_path = f"{self.local_folder_path}/{self.job_id}.jsonl"
        self.trend_keyword_by_entity_file = f"{self.local_folder_path}/{self.job_id}_trend_keywords_by_entity.json"
        
        # hdfs 관련
        self.hdfs = HdfsFileHandler()
        self.hdfs_upload_folder = f"/user/ds/wordpopcorn/{self.lang}/daily/{self.service}_suggest_for_llm_entity_topic/{self.job_id[:4]}/{self.job_id[:6]}/{self.job_id[:8]}/{self.job_id}"
        self.one_week_ago_trend_keywords = self.get_one_week_ago_trend_keywords(self.job_id[:8], lang) # 이전 7일간 나왔던 트렌드 키워드
        
        # Task History 관련
        self.log_task_history = log_task_history
        self.task_history = TaskHistory(postgres_db_config, "trend_finder", self.task_name, self.job_id, self.lang)
        
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

    @error_notifier
    def get_llm_entity_topic(self) -> List[str]:
        '''
        get_llm_entity_topic 리스트 가져오기
        '''
        if self.lang == "ko":
            return QueryDatabaseKo.get_llm_entity_topic()
        if self.lang == "ja":
            return QueryDatabaseJa.get_llm_entity_topic()
        if self.lang == "en":
            return QueryDatabaseEn.get_llm_entity_topic()
        
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
    def get_extension(self) -> List[str]:
        '''
        대상 키워드 있을 경우 확장 텍스트 가져오기
        '''
        lang = self.get_lang(self.lang)
        return lang.suggest_extension_texts_by_rank(0) + lang.suggest_extension_texts_by_rank(1)

    @error_notifier
    def make_check_dict(self, lang:str) -> dict:
        '''
        초성별 완성형 문자 딕셔너리 생성        
        '''
        if lang == "ko":
            letters = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
            # complete_letters = ['옳', '흠', '애', '렬', '싣', '윤', '덱', '튜', '향', '뻔', '필', '겔', '핀', '힌', '쏜', '넘', '웠', '쑤', '정', '캉', '는', '금', '행', '줌', '롱', '현', '초', '팎', '촌', '룸', '뚫', '씹', '슈', '흩', '익', '놨', '봉', '탱', '켜', '비', '켄', '넷', '에', '붙', '봅', '께', '멕', '희', '찼', '쿄', '폈', '님', '즉', '셨', '륜', '섭', '쪽', '둠', '섰', '컷', '넛', '매', '욱', '우', '묘', '놀', '딘', '마', '숯', '뉘', '팠', '착', '굿', '훔', '앓', '납', '얇', '온', '녁', '멸', '알', '재', '낼', '궐', '컬', '툭', '밀', '츄', '밝', '갖', '고', '까', '테', '쯤', '벙', '냥', '교', '멀', '곁', '이', '쉬', '호', '누', '씩', '엣', '풍', '웬', '룹', '슴', '미', '라', '젓', '멈', '윙', '몬', '빈', '꼭', '을', '롬', '슐', '춘', '페', '헌', '웰', '퀸', '뒤', '솔', '훈', '동', '폄', '꺼', '렛', '딱', '긴', '쉴', '쯔', '궤', '뱅', '료', '뚜', '승', '뱀', '굶', '붐', '느', '콰', '걸', '옆', '얻', '탰', '욕', '콥', '팜', '곱', '왔', '캠', '곰', '틋', '즐', '쉼', '왓', '탕', '찔', '박', '캔', '즘', '액', '백', '낳', '뽕', '목', '혜', '게', '골', '닛', '떠', '웅', '듭', '된', '탈', '옌', '낌', '럽', '냉', '흥', '적', '눈', '긍', '펙', '갯', '총', '표', '특', '투', '걷', '풋', '뛴', '힐', '징', '득', '부', '킷', '있', '휘', '례', '푼', '로', '맹', '즌', '괴', '랜', '랫', '클', '킹', '물', '섯', '볍', '뒷', '맏', '썰', '낫', '맵', '려', '청', '기', '람', '쁘', '뜰', '랩', '응', '닙', '잭', '윗', '딧', '낄', '댄', '모', '젠', '찢', '딛', '반', '아', '혐', '벌', '담', '짜', '먹', '펠', '웨', '딜', '끽', '돔', '힙', '굳', '언', '외', '핍', '뜻', '않', '요', '켓', '룰', '밸', '낸', '집', '넸', '듬', '맑', '맷', '같', '칙', '덤', '킬', '쁜', '렙', '펜', '진', '뿌', '륵', '쏟', '내', '혁', '렷', '터', '철', '형', '처', '섬', '똘', '병', '리', '콧', '자', '임', '겁', '져', '빌', '맺', '참', '빅', '멧', '찻', '씁', '땅', '렵', '린', '춤', '얹', '택', '것', '나', '스', '뢰', '취', '갈', '석', '죠', '븐', '역', '딥', '드', '몽', '뷰', '퇴', '낀', '벤', '템', '픽', '캄', '픈', '속', '헤', '따', '뛸', '맞', '념', '건', '뺏', '힘', '쨌', '식', '므', '든', '듀', '군', '섞', '뺀', '졸', '숙', '쩔', '맙', '살', '균', '캣', '짖', '괜', '촉', '겐', '끗', '그', '달', '샵', '짬', '베', '딪', '세', '출', '렐', '냄', '덜', '띈', '샹', '학', '뀐', '뚝', '끊', '푹', '뜬', '몹', '범', '탭', '쉰', '잃', '긁', '민', '뻗', '칭', '쿵', '휠', '걱', '낱', '너', '닉', '쉽', '끈', '촘', '습', '의', '깔', '텨', '길', '피', '만', '칫', '뷔', '완', '써', '볶', '삶', '챌', '심', '숭', '틈', '컸', '림', '퍼', '귀', '쥔', '깊', '릇', '껍', '텅', '딩', '뮬', '준', '날', '씌', '횡', '뤘', '약', '업', '실', '코', '분', '메', '권', '릭', '콤', '프', '빨', '다', '컴', '억', '겨', '쇼', '티', '측', '악', '받', '엘', '퉁', '록', '룻', '쿨', '팸', '대', '붓', '댓', '용', '열', '최', '툼', '읽', '퀄', '파', '럴', '짓', '였', '밖', '깥', '새', '쓴', '빵', '삭', '칩', '산', '운', '별', '극', '콘', '났', '령', '제', '댈', '땐', '웹', '협', '데', '뿔', '펑', '견', '뜸', '눌', '릴', '객', '워', '뀌', '잘', '텔', '류', '랑', '쐐', '뽐', '흙', '첨', '셜', '썩', '뱉', '무', '룡', '뭉', '럿', '옵', '닷', '궈', '굽', '김', '벼', '봐', '얀', '머', '척', '빔', '뇨', '맡', '북', '잠', '뾰', '꿋', '널', '름', '롯', '째', '질', '돼', '능', '봤', '훤', '꾀', '잇', '끔', '휩', '며', '랭', '블', '점', '더', '풀', '타', '십', '직', '뎌', '뺨', '떨', '맨', '틸', '편', '침', '뤼', '쏘', '탤', '졌', '깎', '롭', '위', '문', '곳', '술', '깬', '육', '탁', '씻', '해', '줄', '빙', '카', '증', '인', '끝', '톤', '년', '앱', '늘', '통', '렌', '퀘', '짱', '똑', '선', '곧', '젤', '팝', '잊', '른', '유', '월', '멘', '챙', '등', '펌', '렀', '쥐', '평', '전', '둔', '보', '빡', '펄', '쇠', '룬', '늪', '족', '둘', '렉', '댁', '허', '뭐', '띄', '쫓', '칵', '잔', '랍', '어', '잡', '낡', '밴', '닦', '친', '덮', '멍', '삽', '설', '높', '쿠', '돋', '씽', '뜯', '겠', '꿀', '헛', '셋', '원', '팔', '네', '델', '많', '끼', '흉', '손', '짧', '겸', '넨', '궁', '셸', '깃', '던', '믿', '털', '잖', '계', '샛', '폭', '얘', '륭', '한', '들', '겪', '싶', '썬', '여', '렁', '짤', '쓰', '깝', '퀴', '흘', '센', '글', '돈', '쳤', '련', '넥', '란', '롤', '쎄', '밍', '찌', '삼', '톡', '콕', '커', '핏', '몫', '똥', '뒀', '먼', '남', '큰', '훌', '껑', '구', '곤', '급', '융', '씨', '핵', '샷', '앞', '빛', '즈', '음', '겼', '돕', '녀', '셰', '얄', '옮', '갱', '둑', '차', '륨', '촬', '숲', '궂', '닮', '색', '팡', '곡', '갓', '쌈', '닌', '옛', '펼', '뛰', '꼽', '러', '항', '앰', '톰', '젝', '칼', '캘', '엉', '종', '쿼', '숱', '벚', '래', '케', '룽', '웍', '샀', '찍', '떻', '혔', '둥', '췄', '툰', '딕', '태', '좀', '활', '홀', '샤', '왜', '팁', '를', '팍', '퐁', '밑', '았', '텀', '합', '몇', '뤄', '녕', '헝', '력', '듈', '헨', '관', '푸', '컵', '함', '턱', '뮤', '덕', '켈', '주', '낙', '튀', '줬', '콩', '뻑', '브', '엌', '뿍', '될', '뭘', '꾸', '틴', '묶', '픔', '횟', '엑', '입', '섀', '킥', '앨', '오', '상', '츠', '놓', '맛', '망', '춧', '엽', '명', '쉐', '쭉', '셔', '짠', '농', '킴', '잣', '셀', '찮', '콜', '작', '늄', '벨', '놈', '성', '샐', '야', '과', '늑', '랠', '탄', '럭', '존', '뀔', '키', '랄', '싼', '뭄', '넌', '갇', '험', '왕', '꽃', '장', '벗', '올', '눠', '생', '립', '돌', '소', '말', '뭇', '루', '죽', '일', '션', '꿇', '잉', '잦', '젖', '떼', '넣', '깡', '체', '치', '맴', '솥', '막', '트', '됐', '톱', '홉', '끓', '디', '찾', '꽤', '걀', '연', '듣', '틱', '버', '릿', '가', '땡', '영', '환', '휴', '포', '귤', '론', '본', '헐', '곶', '튼', '격', '즙', '퓨', '독', '굴', '싫', '팀', '엔', '률', '팻', '꽂', '량', '수', '랴', '탑', '톨', '법', '불', '뇌', '벳', '겹', '깐', '캐', '컨', '쁨', '꼬', '죄', '회', '왠', '낚', '띠', '햇', '앉', '옹', '확', '서', '랙', '거', '두', '쇄', '밤', '눔', '뉴', '닫', '뚱', '탐', '녹', '텃', '몰', '첼', '흑', '밌', '젊', '몄', '탬', '흔', '꿔', '퓰', '렘', '큘', '층', '홍', '댐', '둡', '쪼', '쳐', '덴', '배', '쿤', '팅', '륙', '링', '찬', '앗', '당', '떴', '탔', '쌓', '섣', '엠', '감', '갚', '쫄', '팟', '혀', '닝', '깅', '듯', '떡', '냈', '광', '솜', '샘', '펀', '긋', '되', '튿', '얼', '효', '벅', '꽉', '략', '슬', '니', '은', '텍', '펫', '슛', '냅', '윈', '줘', '렇', '썹', '팰', '뜨', '삿', '텐', '렸', '꾼', '옴', '신', '압', '국', '쾌', '붉', '울', '뼈', '화', '팥', '품', '강', '방', '넓', '땄', '할', '슘', '볕', '둬', '뮌', '쏙', '폰', '뿐', '틀', '양', '덧', '멜', '덥', '샴', '떤', '돗', '충', '닭', '촛', '검', '꿨', '간', '판', '번', '팽', '엿', '뽀', '랬', '했', '셈', '삐', '좋', '르', '찜', '싱', '꺾', '낯', '복', '쿡', '옐', '슨', '씀', '염', '쩍', '햄', '엮', '챈', '퉈', '랐', '왼', '단', '섹', '릎', '곽', '절', '쥬', '갤', '낭', '썼', '탓', '춰', '접', '빗', '혼', '짚', '송', '쩡', '순', '꼴', '바', '뜩', '눴', '줍', '넬', '톈', '뱃', '껏', '땀', '좌', '끌', '괌', '쏠', '었', '싹', '폼', '붕', '없', '흰', '씬', '딸', '쌍', '낮', '밭', '덟', '큼', '패', '왈', '웃', '엇', '펴', '꼈', '폐', '굉', '딤', '공', '규', '핸', '찰', '띤', '냐', '앙', '릉', '황', '늦', '툴', '근', '책', '락', '챔', '엎', '윌', '도', '렴', '좁', '헬', '변', '뭔', '벽', '샌', '값', '짐', '훨', '턴', '시', '됨', '꼼', '못', '늠', '램', '으', '봄', '홈', '빽', '런', '딴', '넉', '엄', '논', '밥', '봇', '묻', '썸', '핑', '멋', '텝', '앵', '댔', '띔', '앤', '캡', '늬', '혹', '릅', '뉜', '훼', '밋', '꿈', '덩', '결', '쟁', '첩', '큐', '뻐', '지', '탠', '솟', '폴', '켰', '맥', '흐', '숍', '룩', '굵', '갔', '켐', '떳', '빼', '중', '옥', '쓸', '킨', '갑', '녔', '칠', '핫', '천', '됩', '하', '닐', '볼', '옷', '크', '럼', '잎', '튬', '각', '녘', '면', '깜', '팬', '레', '때', '몸', '슷', '안', '채', '껴', '믹', '밟', '벡', '노', '빚', '후', '끄', '훗', '개', '발', '얽', '난', '암', '싸', '조', '뽑', '토', '셉', '짝', '예', '괄', '겉', '꿰', '놔', '히', '축', '경', '췌', '읍', '짙', '첸', '와', '저', '칸', '쌀', '혈', '팩', '뿜', '쩌', '뗐', '멤', '닿', '묵', '닥', '추', '쑥', '플', '잼', '숨', '꽁', '답', '창', '율', '또', '움', '컫', '및', '첫', '깨', '늙', '사', '숫', '흡', '맘', '빠', '획']
            complete_letters = ['가', '개', '거', '게', '겨', '계', '고', '과', '괴', '교', '구', '궈', '궤', '귀', '규', '그', '기', '까', '깨', '꺼', '께', '껴', '꼬', '꽤', '꾀', '꾸', '꿔', '꿰', '뀌', '끄', '끼', '나', '내', '냐', '너', '네', '녀', '노', '놔', '뇌', '뇨', '누', '눠', '뉘', '뉴', '느', '늬', '니', '다', '대', '더', '데', '뎌', '도', '돼', '되', '두', '둬', '뒤', '듀', '드', '디', '따', '때', '떠', '떼', '또', '뚜', '뛰', '뜨', '띄', '띠', '라', '래', '랴', '러', '레', '려', '례', '로', '뢰', '료', '루', '뤄', '뤼', '류', '르', '리', '마', '매', '머', '메', '며', '모', '묘', '무', '뭐', '뮤', '므', '미', '바', '배', '버', '베', '벼', '보', '봐', '부', '뷔', '뷰', '브', '비', '빠', '빼', '뻐', '뼈', '뽀', '뾰', '뿌', '쁘', '삐', '사', '새', '샤', '섀', '서', '세', '셔', '셰', '소', '쇄', '쇠', '쇼', '수', '쉐', '쉬', '슈', '스', '시', '싸', '써', '쎄', '쏘', '쐐', '쑤', '쓰', '씌', '씨', '아', '애', '야', '얘', '어', '에', '여', '예', '오', '와', '왜', '외', '요', '우', '워', '웨', '위', '유', '으', '의', '이', '자', '재', '저', '제', '져', '조', '좌', '죄', '죠', '주', '줘', '쥐', '쥬', '즈', '지', '짜', '째', '쩌', '쪼', '쯔', '찌', '차', '채', '처', '체', '쳐', '초', '최', '추', '춰', '췌', '취', '츄', '츠', '치', '카', '캐', '커', '케', '켜', '코', '콰', '쾌', '쿄', '쿠', '쿼', '퀘', '퀴', '큐', '크', '키', '타', '태', '터', '테', '텨', '토', '퇴', '투', '퉈', '튀', '튜', '트', '티', '파', '패', '퍼', '페', '펴', '폐', '포', '표', '푸', '퓨', '프', '피', '하', '해', '허', '헤', '혀', '혜', '호', '화', '회', '효', '후', '훼', '휘', '휴', '흐', '희', '히']
            check_dict = {let:[] for let in letters}
            for let in complete_letters:
                check_dict[extract_initial(let)].append(let)
            return check_dict

        elif lang == "ja":
            letters = ['あ', 'い', 'う', 'え', 'お', 'か', 'き', 'く', 'け', 'こ', 'さ', 'し', 'す', 'せ', 'そ', 'た', 'ち', 'つ', 'て', 'と', 'な', 'に', 'ぬ', 'ね', 'の', 'は', 'ひ', 'ふ', 'へ', 'ほ', 'ま', 'み', 'む', 'め', 'も', 'や', 'ゆ', 'よ', 'ら', 'り', 'る', 'れ', 'ろ', 'わ', 'を', 'ん']
            check_dict = {let:[] for let in letters}
            for x in letters:
                for y in letters:
                    check_dict[x].append(x+y)
            return check_dict
        
        elif lang == "en":
            alphabets = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']#, ' ']
            check_dict = {alphabet:[] for alphabet in alphabets}
            for x in alphabets:
                if x == " ": continue
                for y in alphabets:
                    check_dict[x].append(x+y)
            return check_dict
        
        else:
            print(f"[{datetime.now()}] {lang}의 check_dict는 없습니다.")

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
    def load_keywords_from_hdfs(self, file_path):
        """HDFS에서 txt 파일을 읽어와서 키워드 리스트로 반환"""
        try:
            contents = self.hdfs.load(file_path)
            keywords = set([line.strip() for line in contents.splitlines() if line.strip()])  # 중복 제거 및 정렬
            return sorted(keywords)
        except Exception as e:
            print(f"[{datetime.now()}] HDFS에서 파일을 불러올 수 없습니다: {e}")
            return []
        
    @error_notifier
    def get_all_txt_files(self, date_folder_path) -> List[str]:
        '''
        입력한 date_folder_path 하위 경로를 돌면서 .txt 파일 목록을 가져오는 함수
        '''
        all_txt_files = []

        if not self.hdfs.exist(date_folder_path):
            print(f"[{datetime.now()}] {date_folder_path} 경로가 존재하지 않습니다.")
            return all_txt_files
        
        # 현재 폴더의 하위 디렉토리 목록을 가져옴
        job_id_dirs = [d for d in self.hdfs.list(date_folder_path) if not has_file_extension(d)] # 디렉토리만 가져옴

        # 하위 디렉토리 목록을 순회
        for job_id in job_id_dirs:
            job_id_path = f"{date_folder_path}/{job_id}"
            if not self.hdfs.exist(job_id_path):
                continue
            files = self.hdfs.list(job_id_path)
            for file in files:
                if file.endswith("_trend_keywords.txt"):
                    all_txt_files.append(f"{job_id_path}/{file}")

        return all_txt_files

    @error_notifier
    def get_one_week_ago_trend_keywords(self, today, lang):
        '''
        이전 7일 트렌드 키워드 목록 가져오기
        '''
        services = ['google', 'youtube']
        
        try:
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
        except Exception as e:
            print(f"[{datetime.now()}] 이전 7일 트렌드 키워드 목록 가져오기 실패 : {e}")
            return []

    @error_notifier
    def read_already_collected_text(self):
        '''
        이미 수집된 키워드 텍스트 읽기
        '''
        collected_texts = []
        if os.path.exists(self.local_result_path):
            print(f"[{datetime.now()}] 이미 수집된 결과가 있습니다! ({self.local_result_path})")
            for line in JsonlFileHandler(self.local_result_path).read_generator():
                collected_texts.append(line['keyword'])
            collected_texts = list(set(collected_texts))
            print(f"[{datetime.now()}] 이미 수집된 키워드 : {len(collected_texts)}개")
        else:
            print(f"[{datetime.now()}] 이미 수집된 결과가 없습니다. (not found file {self.local_result_path})")
        return collected_texts
    
    @error_notifier
    def get_target_letter_suggest(self, llm_entity_topic:List[str]):
        '''
        대상 키워드 있는 경우 해당 키워드의 0, 1단계 서제스트 수집
        '''
        try:
            target_num_process = 95
            print(f"[{datetime.now()}] 수집 프로세스 개수 : {target_num_process}")
            lang = self.get_lang(self.lang)
            extension_texts = lang.suggest_extension_texts_by_rank(0) + lang.suggest_extension_texts_by_rank(1) # 확장 텍스트 1글자임
            if self.lang == "ja": # 일본의 경우 띄어쓰기 하지 않음
                targets = [topic + t for topic in llm_entity_topic for t in extension_texts] # 서제스트 수집할 키워드 리스트
            else:
                targets = [topic + " " + t for topic in llm_entity_topic for t in extension_texts] # 서제스트 수집할 키워드 리스트

            print(f"[{datetime.now()}] 대상 키워드 0, 1 단계 extension text 추가 후 개수 {len(targets)}")
            self.target_letter_suggest_length = len(targets) # 대상 키워드 0, 1단계 서제스트 수집할 개수
            print(f"[{datetime.now()}] self.target_letter_suggest_length : {self.target_letter_suggest_length}")
            already_collected_texts = self.read_already_collected_text() # 이미 수집한 키워드 읽기
            targets = list(set(targets) - set(already_collected_texts))
            print(f"[{datetime.now()}] 이미 수집된 키워드 제외한 개수 {len(targets)}")
            already_collected_keywords = self.get_already_collected_keywords()
            targets = list(set(targets) - set(already_collected_keywords))
            self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=target_num_process)
            print(f"[{datetime.now()}] 대상 키워드 서제스트 0, 1 단계 수집 완료")
        except Exception as e:
            print(f"[{datetime.now()}] ERROR from get_target_letter_suggest : {e}")
    
    @error_notifier
    def get_target_charactor_suggest(self, llm_entity_topic:List[str]):
        '''
        대상 키워드 있는 경우 해당 키워드의 완성형 서제스트 수집
        한국의 경우 초성의 valid 한 서제스트가 valid_threshold개 이상이라면 해당 초성으로 시작하는 완성형 문자의 서제스트만 수집
        일본의 경우 
        '''
        try:
            valid_threshold = 8
            target_num_process = 95            
            print(f"[{datetime.now()}] 수집 프로세스 개수 : {target_num_process}")
            check_dict = combine_dictionary([self.make_check_dict("ko"), self.make_check_dict("ja"), self.make_check_dict("en")])
            targets = []
            cnt = 0
            for line in JsonlFileHandler(self.local_result_path).read_generator(line_len = self.target_letter_suggest_length): # 대상 키워드의 0, 1 단계만 수집된 상태 (get_target_letter_suggest의 결과)
                cnt += 1
                extension_letter = line['keyword'][-1] # 확장 문자
                target_keyword = line['keyword'][:-1].strip() # 대상 키워드
                if (target_keyword in llm_entity_topic and
                    extension_letter in check_dict): # 해당 문자가 초성인 경우
                    # print(f"\n[{datetime.now()}] 대상 키워드 : {target_keyword} | 확장 문자 : {extension_letter}")
                    if cnt_valid_suggest(line['suggestions'], 
                                            target_keyword=target_keyword, 
                                            extension=extension_letter, 
                                            log=False) >= valid_threshold: # valid한 서제스트가 valid_threshold개 이상이면
                        # print(f"=>😀'{extension_letter}'의 valid한 서제스트 개수가 {valid_threshold}개 이상입니다.\n")
                        # 해당 초성으로 시작하는 완성형 문자의 서제스트만 수집
                        extension_texts = list(set(check_dict[extension_letter]))
                        extension_texts = [t for t in extension_texts if t != ""]
                        if self.lang == "ja":
                            targets += [target_keyword + t for t in extension_texts]
                        else:
                            targets += [target_keyword + " " + t for t in extension_texts]
            targets = list(set(targets))
            print(f"[{datetime.now()}] {self.local_result_path}에서 {cnt}줄 읽음 (self.target_letter_suggest_length : {self.target_letter_suggest_length})")
            print(f"[{datetime.now()}] 대상 키워드 extension text 추가 후 개수 {len(targets)}")
            already_collected_texts = self.read_already_collected_text()
            targets = list(set(targets) - set(already_collected_texts))
            print(f"[{datetime.now()}] 이미 수집된 키워드 제외한 개수 {len(targets)}")
            already_collected_keywords = self.get_already_collected_keywords()
            targets = list(set(targets) - set(already_collected_keywords))
            self.get_suggest_and_request_serp(targets, self.local_result_path, num_processes=target_num_process)
        except Exception as e:
            print(f"[{datetime.now()}] ERROR from get_target_charactor_suggest : {e}")
        else:
            print(f"[{datetime.now()}] 대상 키워드의 완성형 서제스트 수집 완료")

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
    def run_google(self):
        '''
        구글 서제스트 수집
        '''
        # 대상 키워드 서제스트 수집       
        try:
            # 대상 키워드 서제스트 수집할 entity topic 가져오기 (from DB)
            llm_entity_topic = self.get_llm_entity_topic()

            # 1. 대상 키워드 서제스트 수집
            print(f"[{datetime.now()}] 대상 키워드 서제스트 수집 시작 (수집할 entity topic 개수 : {len(llm_entity_topic)})")
            self.get_target_letter_suggest(llm_entity_topic) # 대상 키워드 + 0, 1단계 서제스트 수집
            self.get_target_charactor_suggest(llm_entity_topic) # 대상 키워드 + 완성형, 알파벳 서제스트 수집
            # 압축
            self.local_result_path = GZipFileHandler.gzip(self.local_result_path)
        except Exception as e:
            print(f"[{datetime.now()}] {self.lang} {self.service} 대상 키워드 서제스트 수집 실패 : {e}")
        else:
            print(f"[{datetime.now()}] {self.lang} {self.service} 대상 키워드 서제스트 수집 완료")

    @error_notifier
    def count_trend_keyword(self) -> int:
        try:
            trend_keywords = TXTFileHandler(self.trend_keyword_file).read_lines()
            trend_keywords = list(set(trend_keywords))
            print(f"[{datetime.now()}] {self.lang} {self.service} 트렌드 키워드 개수 : {len(trend_keywords)}")
            return len(trend_keywords)
        except Exception as e:
            print(f"[{datetime.now()}] Error from count_trend_keyword | {self.lang} {self.service} 트렌드 키워드 개수 추출 실패 | {e}")

    @error_notifier
    def upload_to_hdfs(self):
        target_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}.jsonl.gz"
        self.hdfs.upload(source=self.local_result_path, dest=target_hdfs_path, overwrite=True)

        trend_keyword_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}_trend_keywords.txt"
        self.hdfs.upload(source=self.trend_keyword_file, dest=trend_keyword_hdfs_path, overwrite=True)
        
        new_trend_keyword_hdfs_path = f"{self.hdfs_upload_folder}/{self.job_id}_{self.suggest_type}_trend_keywords_new.txt"
        self.hdfs.upload(source=self.new_trend_keyword_file, dest=new_trend_keyword_hdfs_path, overwrite=True)

    @error_notifier
    def extract_trend_keywords_by_entity(self):
        '''
        entity별 트렌드 키워드 추출
        '''
        entities = self.get_llm_entity_topic()
        trend_keywords_by_entity = {}
        if self.local_result_path.endswith(".gz"):
            self.local_result_path = GZipFileHandler.ungzip(self.local_result_path)
        for line in JsonlFileHandler(self.local_result_path).read_generator(): 
            for suggestion in line['suggestions']:
                s_type = suggestion['suggest_type']
                s_subtypes = suggestion['suggest_subtypes']
                keyword = line['keyword']
                entity = ' '.join(keyword.split(' ')[:-1])
                if entity not in entities:
                    print(f"[{datetime.now()}] {entity}는 대상 entity가 아닙니다.")
                if entity not in trend_keywords_by_entity: # 딕셔너리에 entity가 없으면 추가
                    trend_keywords_by_entity[entity] = {}
                if keyword not in trend_keywords_by_entity[entity]: # 딕셔너리에 keyword가 없으면 추가
                    trend_keywords_by_entity[entity][keyword] = []
                if is_trend_keyword(suggestion['text'], s_type, s_subtypes):
                    trend_keywords_by_entity[entity][keyword].append(suggestion['text'])
        JsonFileHandler(self.trend_keyword_by_entity_file).write(trend_keywords_by_entity)
        self.local_result_path = GZipFileHandler.gzip(self.local_result_path)

    @error_notifier
    def update_task_history(self, history:List[Tuple]):
        try:
            PostGres.insert_to_task_history(history, "update")
        except Exception as e:
            print(f"[{datetime.now()}] failed to update task history: {e}")

    def run(self):
        try:
            start_time = datetime.now()
            print(f"job_id : {self.job_id}")
            if self.log_task_history:
                self.task_history.set_task_start()
                self.task_history.set_task_in_progress()
                
            if self.service == "google":
                self.run_google()

            self.count_trend_keyword()

            self.upload_to_hdfs()
            
            if self.log_task_history:
                self.task_history.set_task_completed()
            end_time = datetime.now()
        except Exception as e:
            print(f"[{datetime.now()}] 서제스트 수집 실패 작업 종료\nError Msg : {e}")
            ds_trend_finder_dbgout_error(f"{self.slack_prefix_msg}\nMessage : 서제스트 수집 실패 작업 종료")
            if self.log_task_history:
                self.task_history.set_task_error(error_msg=e)
        else:
            print(f"[{datetime.now()}] 서제스트 수집 완료")
            ds_trend_finder_dbgout(f"{self.slack_prefix_msg}\nMessage : 서제스트 수집 완료\nUpload Path : {self.hdfs_upload_folder}\n{end_time-start_time} 소요")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", help="language", default=None)
    parser.add_argument("--service", help="service(google or youtube)", default=None)
    args = parser.parse_args()
    
    pid = os.getpid()
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