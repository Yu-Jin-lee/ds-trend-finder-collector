import time
from datetime import datetime, timedelta
import dateutil.parser

class DateConverter:
    @staticmethod
    def convert_str_to_datetime(text):
        date_formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%d %H:%M:%S', '%Y. %m. %d.', '%Y%m%d', '%Y%m%d%H', '%Y%m%d%H%M', '%Y/%m/%d', '%b %d, %Y']

        for date_format in date_formats:
            try:
                datetime_object = datetime.strptime(text, date_format)
                return datetime_object
            except ValueError:
                pass
    
        print(f"올바른 날짜 형식이 아닙니다. : {text}")
        return None
    
    @staticmethod
    def convert_datetime_to_str(datetime_value : datetime,
                                format : str = '%Y-%m-%dT%H:%M:%S.%fZ'):
        return datetime_value.strftime(format)

    @staticmethod
    def convert_datetime_to_timestamp(datetime : datetime):
        return int(time.mktime(datetime.timetuple()))
    
    @staticmethod
    def convert_timestamp_to_datetime(timestamp : int):
        return datetime.fromtimestamp(timestamp)
    
def convert_kst(utc_string):
    # datetime 값으로 변환
    dt_tm_utc = dateutil.parser.isoparse(utc_string)

    # +9 시간
    tm_kst = dt_tm_utc + timedelta(hours=9)

    # 일자 + 시간 문자열로 변환
    str_datetime = tm_kst.strftime('%Y-%m-%d %H:%M:%S')

    return str_datetime