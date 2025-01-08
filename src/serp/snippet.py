import re
from datetime import timedelta, datetime
from typing import Optional

from utils.converter import DateConverter
from lang import get_language

class Snippet:
    def __init__(self, 
                 text : str,
                 lang : str):
        self.text = text
        self.lang = lang
        
    @property
    def publication_date(self) -> str:
        '''
        서프에 있는 발행일자 그대로 추출
        ex) ['2021. 7. 1.', '2일 전', '3시간 전']
        '''
        publication_date = PublicationDateExtractor(self.lang).extract_publication_date(self.text)
        if not publication_date : 
            publication_date = ""
        return publication_date
    
    def real_publication_date(self, 
                              base_datetime : datetime # 서프 수집 시간 (request_time)
                              ) -> datetime:
        '''
        서프에 있는 발행일자를 서프 수집 시간을 기준으로 실제 발행일자로 변환
        '''
        publish_date = PublicationDateExtractor(self.lang).extract_publication_date(self.text)
        if publish_date:
            if (publish_date.endswith("전") or 
                publish_date.endswith("前") or
                publish_date.endswith("ago")):
                publish_date = DateCalculator.calculate_datetime_difference(base_datetime, publish_date)
            else:
                publish_date = DateConverter.convert_str_to_datetime(publish_date)
        return publish_date

class PublicationDateExtractor:
    def __init__(self, lang):
        self.lang = get_language(lang)
        
    def find_pattern(self,
                     text, 
                     pattern):
        match = pattern.match(text)
        if match:
            date_pattern = match.group(1)
            date_pattern = date_pattern.rstrip(' —')
            return date_pattern
        else:
            return None
        
    def extract_date_pattern(self,
                             text):
        date_patterns = [r"^([A-Z][a-z]{2} \d{1,2}, \d{4})(?: — )",
                         r"^(\d{4}\. \d{1,2}\. \d{1,2}.)(?: — )",
                         r"^(\d{4}\/\d{2}\/\d{2})(?: — )"]
        
        for pattern in date_patterns:            
            DATE_PATTERN = re.compile(pattern)
            result = self.find_pattern(text, DATE_PATTERN)
            if result != None:
                return result
    
    def extract_time_pattern(self,
                             text):
        time_text = self.lang.interval_map()['h']
        TIME_PATTERN = re.compile(r"(\d+{})(?: — )".format(time_text))
        return self.find_pattern(text, TIME_PATTERN)
    
    def extract_day_pattern(self,
                            text):
        
        day_text = self.lang.interval_map()['d']
        DAY_PATTERN = re.compile(r"(\d+{})(?: — )".format(day_text))
        return self.find_pattern(text, DAY_PATTERN)
    
    def extract_publication_date(self,
                                 text):
        res = self.extract_date_pattern(text)
        if res:
            return res
        res = self.extract_time_pattern(text)
        if res:
            return res
        res = self.extract_day_pattern(text)
        return res

class DateCalculator:
    @staticmethod
    def calculate_datetime_difference(base_datetime : datetime, text : str) -> Optional[datetime]:
        # 텍스트에서 숫자와 단위를 추출
        patterns = [r'(\d+)일 전|(\d+)시간 전',
                    r'(\d+) 日前|(\d+) 時間前',
                    r'(\d+) days ago|(\d+) hours ago']
        for pattern in patterns:
            pattern = re.compile(pattern)
            match = pattern.match(text)
            if match:
                days, hours = match.groups()
                if days:
                    base_datetime -= timedelta(days=int(days))
                elif hours:
                    base_datetime -= timedelta(hours=int(hours))
                return base_datetime
        return None