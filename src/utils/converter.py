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

def adjust_job_id(job_id, hours_to_subtract=14):
    """
    Adjusts the datetime in the given job_id by subtracting the specified number of hours.

    Args:
        job_id (str): A string representing a job ID with the format "YYYYMMDDHH".
        hours_to_subtract (int): The number of hours to subtract from the job ID. Default is 14.

    Returns:
        str: A new job ID with the adjusted datetime.

    Example:
        >>> job_id = "2024120311"
        >>> new_job_id = adjust_job_id(job_id)
        >>> print(new_job_id)
        "2024120221"

        >>> job_id = "2024120102"
        >>> new_job_id = adjust_job_id(job_id, hours_to_subtract=3)
        >>> print(new_job_id)
        "2024113023"
    """
    # Extract the datetime part from job_id
    datetime_part = job_id[:10]
    
    # Convert to a datetime object
    job_datetime = datetime.strptime(datetime_part, "%Y%m%d%H")
    
    # Subtract the specified number of hours
    adjusted_datetime = job_datetime - timedelta(hours=hours_to_subtract)
    
    # Format the adjusted datetime back to the job_id format
    new_job_id = adjusted_datetime.strftime("%Y%m%d%H")
    
    return new_job_id