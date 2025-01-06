import re
import string
from datetime import datetime

from lang.lang_base import LanguageBase
from utils.file import PickleFileHandler
from utils.text import normalize_spaces

class Ko(LanguageBase):
    def __init__(self) -> None:
        super().__init__()
        self.complete_hanguls = PickleFileHandler("./lang/ko/data/characters.pickle").read()
        self.complete_hanguls_small_set = ['가', '개', '거', '게', '겨', '계', '고', '과', '괴', '교', '구', '궈', '궤', '귀', '규', '그', '기', '까', '깨', '꺼', '께', '껴', '꼬', '꽤', '꾀', '꾸', '꿔', '꿰', '뀌', '끄', '끼', '나', '내', '냐', '너', '네', '녀', '노', '놔', '뇌', '뇨', '누', '눠', '뉘', '뉴', '느', '늬', '니', '다', '대', '더', '데', '뎌', '도', '돼', '되', '두', '둬', '뒤', '듀', '드', '디', '따', '때', '떠', '떼', '또', '뚜', '뛰', '뜨', '띄', '띠', '라', '래', '랴', '러', '레', '려', '례', '로', '뢰', '료', '루', '뤄', '뤼', '류', '르', '리', '마', '매', '머', '메', '며', '모', '묘', '무', '뭐', '뮤', '므', '미', '바', '배', '버', '베', '벼', '보', '봐', '부', '뷔', '뷰', '브', '비', '빠', '빼', '뻐', '뼈', '뽀', '뾰', '뿌', '쁘', '삐', '사', '새', '샤', '섀', '서', '세', '셔', '셰', '소', '쇄', '쇠', '쇼', '수', '쉐', '쉬', '슈', '스', '시', '싸', '써', '쎄', '쏘', '쐐', '쑤', '쓰', '씌', '씨', '아', '애', '야', '얘', '어', '에', '여', '예', '오', '와', '왜', '외', '요', '우', '워', '웨', '위', '유', '으', '의', '이', '자', '재', '저', '제', '져', '조', '좌', '죄', '죠', '주', '줘', '쥐', '쥬', '즈', '지', '짜', '째', '쩌', '쪼', '쯔', '찌', '차', '채', '처', '체', '쳐', '초', '최', '추', '춰', '췌', '취', '츄', '츠', '치', '카', '캐', '커', '케', '켜', '코', '콰', '쾌', '쿄', '쿠', '쿼', '퀘', '퀴', '큐', '크', '키', '타', '태', '터', '테', '텨', '토', '퇴', '투', '퉈', '튀', '튜', '트', '티', '파', '패', '퍼', '페', '펴', '폐', '포', '표', '푸', '퓨', '프', '피', '하', '해', '허', '헤', '혀', '혜', '호', '화', '회', '효', '후', '훼', '휘', '휴', '흐', '희', '히']
        self.alphabets = self.get_alphabets()
        self.numbers = self.get_numbers()
        self.characters = self.get_characters()
        self.letters = self.get_letters()
    
    def language(self) -> str: 
        return 'ko'

    @property
    def hl(self) -> str:
        return 'ko'

    @property
    def gl(self) -> str:
        return 'kr'
    
    def serp_location(self) -> str:
        return 'South Korea'
    
    def get_none(self) -> list:
        return [""]
    
    def get_characters(self) -> list:
        return list(self.complete_hanguls)

    def get_letters(self) -> list:
        return ["ㄱ","ㄲ","ㄴ","ㄷ","ㄸ","ㄹ","ㅁ","ㅂ","ㅃ","ㅅ","ㅆ","ㅇ","ㅈ","ㅉ","ㅊ","ㅋ","ㅌ","ㅍ","ㅎ"]

    def get_alphabets(self) -> list:
        return list(string.ascii_lowercase)
        #return frozenset(list(string.ascii_lowercase))

    def get_numbers(self) -> list:
        return [str(n) for n in list(range(0,10))]
        #return frozenset(list(range(0,10)))
    
    def suggest_extension_texts_by_rank(self, rank) -> list:
        if rank == 0:
            return self.get_none()
        elif rank == 1: # 55개
            return self.get_letters() + self.get_alphabets() + self.get_numbers()
        elif rank == 2: # 1320개
            return self.get_characters()
        elif rank == "2_small": # 262개
            return self.complete_hanguls_small_set
        elif rank == 3: # 72,600개
            return [x + y \
                    for x in self.get_characters() \
                    for y in (self.get_letters() + self.get_alphabets() + self.get_numbers())
                    ]
        elif rank == "3_small": # 14,410개
            return [x + y \
                    for x in self.complete_hanguls_small_set \
                    for y in (self.get_letters() + self.get_alphabets() + self.get_numbers())
                    ]
        elif rank == 4: # 1,742,400개
            return [x + y \
                    for x in self.get_characters() \
                    for y in self.get_characters()
                    ]
        elif rank == "4_small": # 68,644개
            return [x + y \
                    for x in self.complete_hanguls_small_set \
                    for y in self.complete_hanguls_small_set
                    ]
        elif rank == "4_small_with_space":
            return [x + y \
                    for x in self.complete_hanguls_small_set + [" "] \
                    for y in self.complete_hanguls_small_set + [" "]
                    if not (x + y).startswith(" ")]
        elif rank == 5:
            return [x + y + z\
                    for x in self.get_characters() \
                    for y in self.get_characters() \
                    for z in self.get_characters()
                    ]
        elif rank == "5_small": # 17,984,728개
            start_time = datetime.now()
            extension_texts = [x + y + z\
                    for x in self.complete_hanguls_small_set \
                    for y in self.complete_hanguls_small_set \
                    for z in self.complete_hanguls_small_set
                    ]
            print(f"5_small_with_space: {len(extension_texts)}개, 소요시간: {datetime.now() - start_time}")
            return extension_texts
        
        elif rank == "5_small_with_space":
            start_time = datetime.now()
            extension_texts = [normalize_spaces(x + y + z)\
                    for x in self.complete_hanguls_small_set + [" "] \
                    for y in self.complete_hanguls_small_set + [" "] \
                    for z in self.complete_hanguls_small_set + [" "]
                    if not (x + y + z).startswith(" ")
                    ]
            extension_texts = list(set(extension_texts))
            extension_texts = [et for et in extension_texts if len(extension_texts)>=3]
            print(f"5_small_with_space: {len(extension_texts)}개, 소요시간: {datetime.now() - start_time}")
            return extension_texts
     
    def suggest_extension_texts(self, 
                                stratgy : str = "all",
                                contain_none : bool = False) -> list:
        if stratgy == "all":
            if contain_none == True:
                return self.get_none() + \
                       self.suggest_extension_texts_by_rank(1) + \
                       self.suggest_extension_texts_by_rank(2) + \
                       self.suggest_extension_texts_by_rank(3) + \
                       self.suggest_extension_texts_by_rank(4)
            else:
                return self.suggest_extension_texts_by_rank(1) + \
                       self.suggest_extension_texts_by_rank(2) + \
                       self.suggest_extension_texts_by_rank(3) + \
                       self.suggest_extension_texts_by_rank(4)
                       
    def is_domestic(self, text:str, ratio = 0.15):
        try:
            encText = text
            hanCount = len(re.findall(u'[\u3130-\u318F\uAC00-\uD7A3]', encText))
            return (hanCount / len(text)) > ratio
        except Exception as e:
            print(e)
            return False
    
    def interval_map(self) -> dict:
        i_map = {'d':'일 전', 'h':'시간 전', 'm':'분 전'}
        return i_map

    
if __name__ == "__main__":
    ko = Ko()
    # print(ko.hl , ko.gl)
    # print(ko.suggest_extension_texts_by_rank(1))

    targets = ko.suggest_extension_texts_by_rank(0) + \
                    ko.suggest_extension_texts_by_rank(1) + \
                    ko.suggest_extension_texts_by_rank("2_small") + \
                    ko.suggest_extension_texts_by_rank("3_small")
    print(targets)