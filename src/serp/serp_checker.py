import re
from abc import ABC, abstractmethod

from serp.serp import Serp

class SerpChecker(ABC):
    def __init__(self, serp:dict):
        """
        문자열에서 특정 국가(한국, 미국, 일본) 언어 문자의 비율을 계산하고 판단하는 클래스

        Args:
            text (str): 분석할 문자열
        """
        self.serp = serp
        self.text = self._extract_text()
        self.total_characters = len(self.text)

    def _extract_text(self) -> str:
        serp = Serp(self.serp)
        all_text = " ".join(serp.titles()) + " ".join(serp.snippets()) # 타이틀, 스니펫 사용
        all_text = all_text.replace(' ', '') # 공백 제거
        return all_text
    
    def _calculate_ratio(self, pattern):
        """
        특정 언어 패턴에 해당하는 문자의 비율을 계산합니다.

        Args:
            pattern (str): 정규 표현식 패턴

        Returns:
            float: 언어 비율 (0과 1 사이)
        """
        if not self.text:
            return 0.0

        matched_characters = re.findall(pattern, self.text)
        matched_count = len(matched_characters)

        return matched_count / self.total_characters if self.total_characters > 0 else 0.0
    
    @abstractmethod
    def is_domestic(self, ratio_threshold:float, return_ratio:bool=False) -> bool:
        ...
        
class SerpCheckerKo(SerpChecker):
    def is_domestic(self, ratio_threshold = 0.05, return_ratio=False):
        """한국어가 ratio_threshold 이상 포함되어 있는지 판단"""
        ratio = self._calculate_ratio(r'[가-힣ㄱ-ㅎㅏ-ㅣ]')
        if return_ratio:
            return ratio >= ratio_threshold, ratio
        return ratio >= ratio_threshold

class SerpCheckerEn(SerpChecker):
    def is_domestic(self, ratio_threshold = 0.2, return_ratio=False):
        """영어가 ratio_threshold 이상 포함되어 있는지 판단"""
        ratio = self._calculate_ratio(r'[A-Za-z]')
        if return_ratio:
            return ratio >= ratio_threshold, ratio
        return ratio >= ratio_threshold

class SerpCheckerJa(SerpChecker):
    def is_domestic(self, ratio_threshold = 0.05, return_ratio=False):
        """히라가나, 가타카나가 ratio_threshold 이상 포함되어 있는지 판단"""
        ratio = self._calculate_ratio(r'[\u3040-\u309F\u30A0-\u30FF]')
        if return_ratio:
            return ratio >= ratio_threshold, ratio
        return ratio >= ratio_threshold
    
if __name__ == "__main__":
    from utils.file import JsonlFileHandler

    for serp in JsonlFileHandler("/data2/yj.lee/git/ds-trend-finder-collector/src/data/result/target/google/ja/2024111207_serp_non_domestic.jsonl").read_generator():
        if not SerpCheckerJa(serp).is_domestic():
            print(serp['search_parameters']['q'])