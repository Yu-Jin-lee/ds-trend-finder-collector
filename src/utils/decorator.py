from functools import wraps
from datetime import datetime

from utils.slack import ds_trend_finder_dbgout_error

def error_notifier(func):
    """
    메소드 실행 중 오류가 발생할 경우 ds_trend_finder_dbgout_error 함수를 사용해 Slack에 에러 메시지를 전송하는 데코레이터.
    메소드 이름도 함께 포함하여 전송합니다.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            method_name = func.__name__
            print(f"[{datetime.now()}] error from method: {method_name}\nError message: {e}")
            # Slack에 에러 메시지 전송
            ds_trend_finder_dbgout_error(f"{self.slack_prefix_msg}\nMethod: {method_name}\nError: {e}")
            raise  # 에러를 다시 발생시켜 상위에서 처리할 수 있도록 함
    return wrapper