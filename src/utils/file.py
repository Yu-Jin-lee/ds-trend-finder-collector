import os
import json
import gzip
import pickle
import jsonlines
import shutil
from typing import Union, List
from datetime import datetime

class PickleFileHandler:
    def __init__(self, path):
        self.path = path

    def read(self):
        with open(self.path, 'rb') as f:
            return pickle.load(f)

    def write(self, obj):
        with open(self.path, 'wb') as f:
            pickle.dump(obj, f)

class JsonFileHandler:
    def __init__(self, path):
        self.path = path

    def read(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                data = json.load(f)
            return data
        else:
            print(f"[{self.path}] not exist")
            
    def write(self, data):
        try:
            with open(self.path, 'w') as file: #  JSON 파일을 쓰기 모드로 엽니다.
                json.dump(data, file, indent=4) # 파이썬 객체를 JSON 형식으로 인코딩하여 파일에 씁니다.
        except Exception as e:
            print(e)
            
class JsonlFileHandler:
    def __init__(self, path):
        self.path = path
    
    def read(self, line_len : int = None):
        data = []
        cnt = 0
        try:
            with jsonlines.open(self.path) as f:
                for line in f:
                    cnt += 1
                    data.append(line)
                    if (line_len != None) & (line_len == cnt):
                        break
        except Exception as e:
            print(self.path, str(e))
        finally:
            return data
        
    def read_generator(self, line_len : int = None):
        '''
        데이터를 한줄씩 읽어서 반환하는 제너레이터
        '''
        cnt = 0
        try:
            with jsonlines.open(self.path) as f:
                for line in f:
                    cnt += 1
                    yield line
                    if (line_len != None) & (line_len == cnt):
                        break
        except Exception as e:
            print(self.path, str(e))
        
    
    def write(self, data : Union[dict, List[dict]]):
        try:
            save_folder = "/".join(self.path.split('/')[:-1])
            if not os.path.exists(save_folder):
                os.makedirs(save_folder)
            if type(data) == dict: # 하나의 데이터 입력
                with open(self.path, "a", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False) # ensure_ascii로 한글이 깨지지 않게 저장
                    f.write("\n") # json을 쓰는 것과 같지만, 여러 줄을 써주는 것이므로 "\n"을 붙여준다.
            elif type(data) == list: # 여러 데이터 입력
                with open(self.path, "a", encoding="utf-8") as f:
                    for d in data:
                        json.dump(d, f, ensure_ascii=False) # ensure_ascii로 한글이 깨지지 않게 저장
                        f.write("\n") # json을 쓰는 것과 같지만, 여러 줄을 써주는 것이므로 "\n"을 붙여준다.
            else:
                print(f"[{datetime.now()} {self.path}에 {len(data)}개 데이터 저장 실패. 데이터 타입은 [dict, List[dict]]이어야 합니다. (input data 타입 : {type(data)})")
        except Exception as e:
            print(self.path, str(e))

class GZipFileHandler:
    @staticmethod
    def gzip(file : str):
        try:
            cmd = f"gzip {file}"
            os.system(cmd)
        except Exception as e:
            print(f"fail gzip [{file}] file", e)
            return None
        else:
            print(f"success gzip [{file}] file")
            return file + '.gz'
    @staticmethod
    def ungzip(file : str):
        try:
            if file.endswith(".gz"):
                cmd = f"gzip -d {file}"
                os.system(cmd)
            else:
                print(f"not .gz file : {file}")
                return None
        except Exception as e:
            print(f"fail ungzip [{file}] file", e)
            return None
        else:
            print(f"success ungzip [{file}] file")
            return file[:-3] # 뒤에 ".gz" 빼고 반환
            
class TXTFileHandler:
    def __init__(self, path):
        self.path = path

    def write(self, data, log:bool=False):
        try:
            with open(self.path, 'a') as file:
                if type(data) == list:
                    data_cnt = len(data)
                    for d in data:
                        file.write(d + '\n')
                else:
                    data_cnt = 1
                    file.write(data + '\n')
            if log:
                print(f"[{datetime.now()}] 파일에 쓰기를 완료했습니다. 총 {data_cnt}개 데이터")
        except Exception as e:
            print(f"[{datetime.now()}] 파일 쓰기 중 오류가 발생했습니다:", e)
            
    def read_lines(self):
        try:
            with open(self.path, 'r') as file:
                lines = file.readlines()
                lines = [line.rstrip() for line in lines] # 줄바꿈 문자 제거
            return lines
        except Exception as e:
            print("파일 읽기 중 오류가 발생했습니다:", e)
    
    def read_generator(self):
        try:
            with open(self.path, 'r') as file:
                while True:
                    line = file.readline()
                    if not line:
                        break
                    yield line.strip()  # .strip()은 줄 끝의 개행 문자를 제거합니다.

        except Exception as e:
            print("파일 읽기 중 오류가 발생했습니다:", e)
                   
def zip_folder(folder):
    cmd = f"zip -r {folder}.zip {folder}"
    os.system(cmd)
    return f"{folder}.zip"

def unzip_folder(zip_folder, dest_folder):
    '''
    zip_folder안에 있는 파일 혹은 폴더들을 dest_folder에 압축 풀기
    '''
    cmd = f"unzip -j {zip_folder} -d {dest_folder}"
    os.system(cmd)
    return dest_folder

def remove_folder(folder):
    cmd = f"rm -rf {folder}"
    os.system(cmd)

def find_files_by_format(folder_path,
                        format):
    # 폴더 내의 모든 파일 및 하위 폴더 리스트 가져오기
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        all_files.extend([os.path.join(root, file) for file in files])

    # 'format'로 끝나는 파일 필터링
    format_files = [file for file in all_files if file.endswith(format)]

    return format_files

def extract_all_gzip_files_in_folder(folder_path):
    '''
    folder_path 폴더에 있는 모든 gzip 파일 압축 해제
    '''
    # 폴더 내의 모든 파일 및 하위 폴더 리스트 가져오기
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        all_files.extend([os.path.join(root, file) for file in files])

    # 'jsonl.gz'로 끝나는 파일 필터링 및 압축 해제
    for file_path in filter(lambda x: x.endswith('.jsonl.gz'), all_files):
        # 압축 해제된 파일의 경로 설정 (확장자 .jsonl.gz를 제외)
        dest_path = os.path.splitext(file_path)[0]

        # gzip 파일 열기
        print("file_path:", file_path)
        with gzip.open(file_path, 'rb') as gz_file:
            # 압축 해제된 파일로 복사
            with open(dest_path, 'wb') as dest_file:
                shutil.copyfileobj(gz_file, dest_file)

        print(f'압축 해제 완료: {file_path} -> {dest_path}')
        
def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def has_file_extension(path, log:bool=False):
    '''
    파일 확장자가 있는지 확인하는 함수
    '''
    _, ext = os.path.splitext(path)  # 확장자 추출
    if ext != "":
        if log:
            print(ext)
    return ext != ""  # 확장자가 있으면 True, 없으면 False
   
if __name__ == "__main__":
    # TXTFileHandler('./test.txt').write(['test','test','test'])
    data = TXTFileHandler('./test.txt').read_lines()
    print(data)