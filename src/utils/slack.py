import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from dataclasses import dataclass

from utils.function import current_function_name

load_dotenv()

@dataclass
class TrendFinderCollectConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder'

@dataclass
class TrendFinderCollectErrorConfig:
    token:str=os.getenv('SLACK_API_TOKEN_DAILY_COLLECT_BOT')
    channel:str='#ds-trend-finder-error'

def post_message(token, channel, text):
    response = requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer "+token},
        data={"channel": channel,"text": text}
    )
    if not json.loads(response.text)["ok"]:
        raise Exception(response.text)

def ds_trend_finder_dbgout(message):
    try:
        strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
        post_message(TrendFinderCollectConfig.token, TrendFinderCollectConfig.channel, strbuf)
    except Exception as e:
        print(f"error from {current_function_name()} : {e}")


def ds_trend_finder_dbgout_error(message):
    try:
        strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
        post_message(TrendFinderCollectErrorConfig.token, TrendFinderCollectErrorConfig.channel, strbuf)
    except Exception as e:
        print(f"error from {current_function_name()} : {e}")