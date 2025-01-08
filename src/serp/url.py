import tldextract
from urllib.parse import urlparse

class URL:
    HOST_SPLITOR = '.'
    PATH_SPLITOR = '/'
    
    def __init__(self, url):
        self.url = url
        parsed_url = urlparse(url)
        self._parsed_url = parsed_url
    
    @property
    def subdomain(self):
        return tldextract.extract(self.url).subdomain
    
    @property
    def domain(self):
        return tldextract.extract(self.url).domain
    
    @property
    def top_level_domain(self):
        return tldextract.extract(self.url).suffix
    
    @property
    def host(self):
        return self._parsed_url.netloc
    
    @property
    def path(self):
        return self._parsed_url.path
    
    @property
    def params(self):
        return self._parsed_url.params
    
    @property
    def query(self):
        return self._parsed_url.query

    @property
    def fragment(self):
        return self._parsed_url.fragment
    
    @property
    def scheme(self):
        return self._parsed_url.scheme
    
    def is_only_host_url(self):
        '''
        호스트로만 이루어진 url인지 체크
        '''
        if (self._parsed_url.path == '') or (self._parsed_url.path == '/'):
            if (self.params == '') and (self.query == '') and (self.fragment == ''):
                return True
        return False
    
    def to_host_url(self):
        '''
        호스트로만 이루어진 url로 변환
        '''
        if self.is_only_host_url():
            return self.url
        return self.scheme + '://' + self.host
    
    def url_without_protocol(self):
        '''
        프로토콜을 제외한 url 반환
        ex) "https://yoondii.tistory.com/118" => "yoondii.tistory.com/118"
        '''
        return self._parsed_url._replace(scheme='').geturl().replace("//", "")
    
    def is_namu_wiki(self) -> bool:
        '''
        나무위키, 위키 url인지 확인
        '''
        namu_wiki_domain_list = [
                                 'namu', # 'https://namu.wiki'
                                 'wikipedia', # 'https://ko.wikipedia.org', 'https://ko.m.wikipedia.org'
                                 'thewiki' # "https://thewiki.kr/"
                                ] 
        if self.domain in namu_wiki_domain_list:
            return True
        return False
    
    def is_pdf_url(self) -> bool:
        '''
        pdf 인지 확인
        '''
        if self.url.endswith(".pdf"):
            return True
        return False
    
    def is_youtube_url(self) -> bool:
        '''
        youtube 인지 확인
        '''
        if self.domain == 'youtube':
            return True
        return False
    
    def is_sns_url(self) -> bool:
        sns_domains = ['twitter', 'instagram', 'facebook', 'tiktok']
        if self.domain in sns_domains:
            return True
        return False
        
    def is_google_play_url(self) -> bool:
        if self.host == "play.google.com":
            return True
        return False