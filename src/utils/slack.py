import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from dataclasses import dataclass

from utils.function import current_function_name

load_dotenv()

@dataclass
class TrendFinderCollectKrConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-kr'

@dataclass
class TrendFinderCollectErrorKrConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-error-kr'

@dataclass
class TrendFinderCollectJpConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-jp'

@dataclass
class TrendFinderCollectErrorJpConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-error-jp'

@dataclass
class TrendFinderCollectUsConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-us'

@dataclass
class TrendFinderCollectErrorUsConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-error-us'


def post_message(token, channel, text):
    response = requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer "+token},
        data={"channel": channel,"text": text}
    )
    if not json.loads(response.text)["ok"]:
        raise Exception(response.text)

def ds_trend_finder_dbgout(lang:str, message:str):
    if lang == "ko":
        TrendFinderCollectConfig = TrendFinderCollectKrConfig
    elif lang == "ja":
        TrendFinderCollectConfig = TrendFinderCollectJpConfig
    elif lang == "en":
        TrendFinderCollectConfig = TrendFinderCollectUsConfig
    else:
        print(f"[{datetime.now()}] 해당 언어 {lang}의 슬랙 채널이 존재하지 않습니다.")
        return

    try:
        strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
        post_message(TrendFinderCollectConfig.token, TrendFinderCollectConfig.channel, strbuf)
    except Exception as e:
        print(f"error from {current_function_name()} : {e}")


def ds_trend_finder_dbgout_error(lang, message):
    if lang == "ko":
        TrendFinderCollectErrorConfig = TrendFinderCollectErrorKrConfig
    elif lang == "ja":
        TrendFinderCollectErrorConfig = TrendFinderCollectErrorJpConfig
    elif lang == "en":
        TrendFinderCollectErrorConfig = TrendFinderCollectErrorUsConfig

    else:
        print(f"[{datetime.now()}] 해당 언어 {lang}의 슬랙 채널이 존재하지 않습니다.")
        return
    
    try:
        strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
        post_message(TrendFinderCollectErrorConfig.token, TrendFinderCollectErrorConfig.channel, strbuf)
    except Exception as e:
        print(f"error from {current_function_name()} : {e}")

def flag_emoji(lang:str):
    flag_emoji_dict = {
            "ko": ":flag-kr:",
            "ja": ":flag-jp:",
            "en": ":flag-us:",
        }
    
    if lang not in flag_emoji_dict:
        print(f"[{lang}]의 flag emoji가 존재하지 않습니다.")
        return None
    
    return flag_emoji_dict[lang]