import os
import pickle
import urllib
from hdfs import InsecureClient
from datetime import datetime

from utils.file import has_file_extension

class HdfsFileHandler:
    def __init__(self):
        self.connect_to_available_host()

    def connect_to_available_host(self):
        self.host = "http://master001.hadoop.prod.ascentlab.io:50070;http://master002.hadoop.prod.ascentlab.io:50070"
        self.user = "ds"
        self.client = InsecureClient(self.host, user=self.user)

    def check_connection(self):
        # print(f"[{datetime.now()}] Checking HDFS connection...")
        # Check if the client is connected by trying a simple operation
        try:
            self.client.list('/')  # Test connection
        except Exception:
            print(f"[{datetime.now()}] Reconnecting to available HDFS host...")
            self.connect_to_available_host()

    def list(self, path):
        self.check_connection()
        return self.client.list(path)
    
    def list_dir(self, path):
        self.check_connection()
        path_url = urllib.parse.quote(path)
        try:
            if self.client.status(path_url):
                return [f"{path}/{p}" for p in self.client.list(path_url, status=False)]
        except Exception as e :
            print(f"[{datetime.now()}] hdfs에 '{path_url}' 경로 없음 (error msg : {e})")
            return []
    
    def load(self, path, encoding="utf-8"):
        self.check_connection()
        with self.client.read(path, encoding=encoding) as f:
            contents = f.read()
        return contents

    def load_line(self, path, encoding="utf-8"):
        self.check_connection()
        with self.client.read(path, encoding=encoding) as f:
            while True:
                contents = f.readline()
                if contents == '':
                    break
                yield contents

    def load_by_user(self, path):
        """
        :param: path    the path which has the prefix as user root hdfs path(/user/[HDFS_USER_NAME])
                        e.g., 'path' from /user/[HDFS_USER_NAME]/'path'
        """
        self.check_connection()
        with self.client.read(path, encoding="utf-8") as f:
            contents = f.read()
        return contents
    
    def load_pickle(self, path):
        self.check_connection()
        with self.client.read(path) as reader:
            bt_contents = reader.read()
            contents = pickle.load(bt_contents)
        return contents

    def loads_pickle(self, path):
        self.check_connection()
        with self.client.read(path) as f:
            contents = f.read()
        pkl_obj = pickle.loads(contents)
        return pkl_obj

    def dumps_pickle(self, path, obj):
        self.check_connection()
        contents = pickle.dumps(obj)
        self.client.write(path, data=contents)
    
    def mkdirs(self, path):
        self.check_connection()
        self.client.makedirs(path)

    def write(self, path, contents, encoding='utf-8', append=False):
        self.check_connection()
        self.client.write(path, data=contents, encoding=encoding, append=append)

    def exist(self, path):        
        self.check_connection()
        return self.client.status(path, strict=False)

    def upload(self, source, dest, overwrite=False):
        self.check_connection()
        if os.path.exists(source):
            if not has_file_extension(dest, True): # 파일 확장자가 없을 경우 폴더 생성
                if not self.exist(dest):
                    self.client.makedirs(dest)
            self.client.upload(hdfs_path=dest, local_path=source, overwrite=overwrite)
            print(f'{source} -> {dest} Uploaded')
        else:
            print(f"Source Not Exist Error: {source}")
            raise FileNotFoundError

    def download(self, source, dest, overwrite : bool = True):
        self.check_connection()
        if self.exist(source):
            if not os.path.exists(dest):
                os.mkdir(dest)
            self.client.download(hdfs_path=source, local_path=dest, overwrite=overwrite)
            print(f'{source} -> {dest} Downloaded')
        else:
            print(f"Source Not Exist Error: {source}")
            raise FileNotFoundError
        
    def last_modified_folder(self, folder_path):
        self.check_connection()
        # 폴더 안의 모든 항목 가져오기
        contents = self.client.list(folder_path)

        # 최신 폴더 정보 초기화
        latest_folder = None
        latest_modification_time = 0

        # 각 항목에 대해 최신 수정 시간인지 확인
        for item in contents:
            item_path = os.path.join(folder_path, item)
            item_stats = self.client.status(item_path)

            # 폴더인 경우에만 검사
            if item_stats['type'] == 'DIRECTORY':
                modification_time = item_stats['modificationTime']

                # 최신 수정 시간인 경우 업데이트
                if modification_time > latest_modification_time:
                    latest_modification_time = modification_time
                    latest_folder = item

        # 최신 폴더 출력
        print("Latest folder:", latest_folder)
        
        return latest_folder
    
    def read_jsonl_generator(self, 
                             hdfs_file_path,
                             local_download_folder_path):
        '''
        hdfs에 있는 파일을 로컬에 다운로드 받아서 한줄씩 읽는 제너레이터
        다 읽은 후 해당 파일 로컬에서 삭제
        '''
        self.check_connection()
        try:
            # 다운로드
            local_file_path = None
            if not os.path.exists(local_download_folder_path):
                os.makedirs(local_download_folder_path)
            self.download(source=hdfs_file_path, dest=local_download_folder_path)
            local_file_path = os.path.join(local_download_folder_path, hdfs_file_path.split("/")[-1])
            
            # 압축 풀기
            from utils.file import GZipFileHandler
            if local_file_path.endswith(".gz"):
                local_file_path = GZipFileHandler.ungzip(local_file_path)
            
            # 파일 읽기
            from utils.file import JsonlFileHandler
            if local_file_path.endswith(".jsonl"):
                for line in JsonlFileHandler(local_file_path).read_generator():
                    yield line
        except Exception as e:
            print(f"error from read_jsonl_generator: {e}")
        finally:
            from utils.file import remove_folder
            if local_file_path != None:
                if os.path.exists(local_file_path):
                    remove_folder(local_file_path)

if __name__ == "__main__":
    hdfs = HdfsFileHandler()
    result = hdfs.list("/user/ds/wordpopcorn/ko/daily/google_suggest_for_llm_entity_topic/2024/202411/20241108")
    print(result)