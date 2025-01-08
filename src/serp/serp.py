import re
import tldextract
from typing import List, Union

from serp.card import Card

class Serp:
    def __init__(self, serp):
        self.serp = serp
        self.lang = serp['search_parameters']['hl']
        self.keyword = serp['search_parameters']['q']
        self.request_time = serp['search_parameters']['requested_time']

    @property
    def cards(self) -> List[Card]:
        return self.extract_cards()
    
    def extract_cards(self):
        cards = []
        if 'features' in self.serp:
            for feature in self.serp['features']:
                cards.append(Card(feature, self.lang))
                
        return cards
    
    def feature_types(self) -> List[str]:
        feature_types = []
        if 'features' in self.serp:
            for feature in self.serp['features']:
                if 'type' in feature:
                    feature_types.append(feature['type'])
                else:
                    feature_types.append("")
        return feature_types
    
    def extract_url(self, card) -> str:
        '''
        하나의 카드에서 url 추출
        '''
        result = ''
        # 기본
        if 'url' in card:
            result = card['url']
        # 추천 스니펫
        elif card['type'] == 'featured_snippet':
            if 'results' in card:
                if 'url' in card['results'][0]:
                    result = card['results'][0]['url']
        return result
    
    ### title 관련 함수 ###
    def extract_title(self, card) -> Union[str, List[str]]:
        card = Card(card, self.lang)
        return card.title
    
    def extract_snippet(self, card):
        snippet = ""
        if 'snippet' in card:
            snippet = card['snippet']
        return snippet
        
    def urls(self) -> List[str]:
        urls = []
        if 'features' in self.serp:
            features = self.serp['features']
            for feature in features:
                urls.append(self.extract_url(feature))
        return urls

    def domains(self) -> List[str]:
        domains = [tldextract.extract(url).domain for url in self.urls()]
        return domains
    
    def site_names(self) -> List[str]:
        site_names = []
        if 'features' in self.serp:
            features = self.serp['features']
            for feature in features:
                if 'site_name' in feature:
                    site_names.append(feature['site_name'])
                else:
                    site_names.append("")
        return site_names
    
    def titles(self) -> List[str]:
        titles = []
        if 'features' in self.serp:
            features = self.serp['features']
            for feature in features:
                titles.append(self.extract_title(feature))
        return titles
    
    def snippets(self) -> List[str]:
        snippets = []
        if 'features' in self.serp:
            features = self.serp['features']
            for feature in features:
                snippets.append(self.extract_snippet(feature))
        return snippets
    
if __name__ == "__main__":
    from utils.file import JsonlFileHandler
    i = 0
    for serp in JsonlFileHandler("/data2/yj.lee/git/ds-trend-finder-collector/src/data/result/target/google/ko/2024111200_serp.jsonl").read_generator():
        i += 1
        # if i==3:break
        keyword = serp['search_parameters']['q']
        if keyword != "오늘 의 부고" : continue
        print(f"[keyword] {keyword}")
        serp = Serp(serp)
        print(f"[title]")
        print(serp.titles())
        print(f"[snippet]")
        print(serp.snippets())
        print(f"==================================\n")
    print(f"총 {i}개의 serp를 읽었습니다.")
