from typing import List, Union

from serp.url import URL
from serp.snippet import Snippet

class Card:
    def __init__(self, card : dict, lang : str):
        if isinstance(card, dict):
            self.lang = lang
            if 'sequence' in card:
                self.sequence = card['sequence']
            self.card = card
            self._url : Union[URL, List[URL]] = self.extract_url()
            self._title = self.extract_title()
            self._snippet : Snippet = self.extract_sinppet()
        else:
            raise TypeError("card must be dict type")
            
    @property
    def url(self) -> Union[URL, List[URL]]:
        return self._url
    
    @property 
    def snippet(self):
        return self._snippet
    
    @property
    def title(self):
        return self._title
    
    def extract_title(self) -> Union[str, List[str]]:
        result = ''
        # 1. title이 여러개 있는 피처 타입 -> List[str]
        ## video_results
        if (self.card['type'] == 'video_results'):
            if "videos" in self.card:
                result = " ".join([video['title'] for video in self.card['videos'] if 'title' in video])
            if "items" in self.card:
                result = " ".join([item['title'] for item in self.card['items'] if 'title' in item])
        ## top_stories
        if (self.card['type'] == 'top_stories'):
            if "items" in self.card:
                for item in self.card['items']:
                    # multi carousels : https://ascentkorea.atlassian.net/wiki/spaces/CJHZ/pages/400424962/Top+stories#Items
                    if "carousels" in item:
                        result += " ".join([car['title'] for car in item['carousels']])
                        result += " "
                    # items : https://ascentkorea.atlassian.net/wiki/spaces/CJHZ/pages/400424962/Top+stories#Items
                    else:
                        result += f" {item['title']}"
            # single carousels : https://ascentkorea.atlassian.net/wiki/spaces/CJHZ/pages/400424962/Top+stories#Single-carousels
            if "carousels" in self.card:
                result += " ".join([car['title'] for car in self.card['carousels'] if 'title' in car])
            
        # 2. 타이틀이 하나인 피처 타입 -> str
        elif 'title' in self.card:
            result = self.card['title']
        elif (self.card['type'] == "featured_snippet" and
              "results" in self.card):
            if 'title' in self.card['results'][0]:
                result = self.card['results'][0]['title']
        # 3. 타이틀이 없는 피처 타입 -> ''
        else:
            result = ''

        return result
        
    def extract_url(self) -> Union[URL, List[URL]]:
        url = ''
        # 기본
        if 'url' in self.card:
            url = self.card['url']
        # 추천 스니펫
        elif self.card['type'] == 'featured_snippet':
            if 'results' in self.card:
                if 'url' in self.card['results'][0]:
                    url = self.card['results'][0]['url']
        # # 아티클(뉴스)
        if self.card['type'] == 'articles':
            urls = []
            if "cards" in self.card:
                for card in self.card["cards"]:
                    urls.append(URL(card["url"]))
            if "items" in self.card:
                for card in self.card["items"]:
                    if "carousels" in card:
                        for carousel in card["carousels"]:
                            urls.append(URL(carousel['url']))
            return urls
        return URL(url)
    
    def extract_sinppet(self) -> Snippet:
        if 'snippet' in self.card:
            snippet_text = self.card['snippet']
        else:
            snippet_text = ""
        return Snippet(snippet_text, self.lang)