import os
import time
from typing import List
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from utils.hdfs import HdfsFileHandler

class GoogleTrendCollector:
    def __init__(self, 
                 lang:str,
                 jobid:str=datetime.now().strftime("%Y%m%d%H"),
                 save_root_folder:str=f'./data/result/google_trend') -> None:
        print(f"[{datetime.now()}] Start set google trend collector")
        self.lang = lang
        print(f"[{datetime.now()}] lang: {self.lang}")
        print(f"[{datetime.now()}] save_root_folder: {save_root_folder}")
        self.save_folder = f"{save_root_folder}/{self.lang}"
        if not os.path.exists(self.save_folder):
            os.makedirs(self.save_folder)
        self.jobid = jobid
        print(f"[{datetime.now()}] jobid: {self.jobid}")
        self.save_path = f"{self.save_folder}/{self.jobid}.txt"
        print(f"[{datetime.now()}] save_path: {self.save_path}")
        
        # Chrome 옵션 설정
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")  # 브라우저 창 없이 실행
        self.chrome_options.add_argument("--disable-gpu")  # GPU 가속 비활성화 (리눅스에서 필수)
        self.chrome_options.add_argument("--window-size=1920x1080")  # 화면 크기 지정 (일부 웹사이트에서 필요)
        self.chrome_options.add_argument("--no-sandbox")  # 리눅스 환경에서 필요할 수 있음
        self.chrome_options.add_argument("--disable-dev-shm-usage")  # 메모리 부족 방지

        self.hdfs = HdfsFileHandler()
        self.hdfs_upload_folder = f"/user/ds/wordpopcorn/{self.lang}/daily/google_trend/{self.jobid[:4]}/{self.jobid[:6]}/{self.jobid[:8]}"
        print(f"[{datetime.now()}] hdfs_upload_folder: {self.hdfs_upload_folder}")

        self.url = f'https://trends.google.co.kr/trending?geo={self.get_geo()}'
        print(f"[{datetime.now()}] url: {self.url}")

    def get_geo(self) -> str:
        geo_map = {"ko": "KR", "ja": "JP", "en": "US-NY"}
        if self.lang not in geo_map:
            raise Exception(f"GEO is not set: {self.lang}")
        return geo_map[self.lang]
        
    def upload_hdfs(self):
        self.hdfs.upload(source=self.save_path,
                         dest=self.hdfs_upload_folder,
                         overwrite=True)
        print(f"[{datetime.now()}] Finish upload google trend keywords to hdfs.")

    def run(self) -> List[str]:
        trends = []
        try:
            start_time = datetime.now()
            print(f"[{datetime.now()}] Start collect google trend keywords.")
            # Selenium WebDriver 실행 (headless 모드)
            driver = webdriver.Chrome(options=self.chrome_options)

            # 웹페이지 열기
            driver.get(self.url)
            time.sleep(5)
            # class가 'mZ3RIc'인 모든 <div> 요소를 찾기

            for inx in range(10):
                time.sleep(2)
                elements = driver.find_elements(By.CLASS_NAME, 'mZ3RIc')

                for element in elements:
                    if element.text not in trends:
                        trends.append(element.text)

                button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='다음 페이지로 이동']")
                button.click()

            with open(self.save_path, 'w', encoding='utf-8') as file:
                for trend in trends:
                    file.write(trend + '\n')
            # WebDriver 종료
            driver.quit()
            print(f"[{datetime.now()}] Finish collect google trend keywords. (processing time: {datetime.now() - start_time})")

            self.upload_hdfs()
            
        except Exception as e:
            print(f"[{datetime.now()}] ❌Error ({e})")

        return trends
    
import re

def filter_google_trend_keywords_ko(keywords):
    filtered_keywords = []
    pattern = re.compile(r'^[가-힣a-zA-Z0-9\s\-_,]+$')  # 한글, 영어, 숫자, 공백, -, _, , 허용
    
    for keyword in keywords:
        if not pattern.match(keyword):
            print(f"Filtered out: {keyword}")  # 필터링된 키워드 출력
        else:
            filtered_keywords.append(keyword)
    
    return filtered_keywords

def filter_google_trend_keywords_ja(keywords):
    filtered_keywords = []
    pattern = re.compile(r'^[ぁ-ゖァ-ヴー々〆一-龥0-9a-zA-Z\s\-_,・.]+$')  # '.' 추가

    for keyword in keywords:
        if not pattern.fullmatch(keyword):  # fullmatch 사용해 전체 문자열 검사
            print(f"Filtered out: {keyword}")  # 필터링된 키워드 출력
        else:
            filtered_keywords.append(keyword)
    
    return filtered_keywords

def filter_google_trend_keywords_en(keywords):
    filtered_keywords = []
    pattern = re.compile(r"^[a-zA-Z0-9\s\-_,.'&–—]+$")  # en dash(–)와 em dash(—) 추가

    for keyword in keywords:
        if not pattern.fullmatch(keyword):
            print(f"Filtered out: {keyword}")  # 필터링된 키워드 출력
        else:
            filtered_keywords.append(keyword)

    return filtered_keywords