import json
import requests
from tenacity import retry, stop_after_attempt
from http import HTTPStatus
from multiprocessing import Pool
from typing import TYPE_CHECKING
from dataclasses import dataclass

from lang import Ko, Ja, En

if TYPE_CHECKING:
    from lang import LanguageBase

@dataclass(frozen=True)
class SuggestApiParams:
    query: str
    hl: str 
    gl: str  
    expand_mode: str
    pre_expand_keyword: str = None
    ds:str = 'google'
    usage_id:str= "yujin.lee"

@retry(stop=stop_after_attempt(10))
def get_suggestions(suggest_api_params):
    url = "http://google-suggest-api.ascentlab.io/api/suggest/v2/suggestions"
    suggestions = []

    if suggest_api_params.ds == "youtube":
        _payload = {
            "q": f"{suggest_api_params.query}",
            "hl": f"{suggest_api_params.hl}",
            "gl": f"{suggest_api_params.gl}",
            "ds": f"{suggest_api_params.ds}",
            "usage_id": f"{suggest_api_params.usage_id}"
        }
    else:
        _payload = {
            "q": f"{suggest_api_params.query}",
            "hl": f"{suggest_api_params.hl}",
            "gl": f"{suggest_api_params.gl}",
            "usage_id": f"{suggest_api_params.usage_id}"
        }

    if suggest_api_params.pre_expand_keyword:
        _payload["q"] = suggest_api_params.query
        _payload["pre_expand_keyword"] = suggest_api_params.pre_expand_keyword

    payload = json.dumps(_payload)
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=20)
        status_code = response.status_code

        if status_code == HTTPStatus.OK:
            suggestions = json.loads(response.text)
            return suggestions
        else:
            print(f"Failed to get suggestions - retrying: {_payload}")
            raise Exception("API response error!")
    except Exception as e:
        print(f"Request failed: {e}")
        raise

# 최대 10번 재시도 후 실패할 경우 None 반환
def fetch_suggestions(suggest_api_params):
    try:
        return get_suggestions(suggest_api_params)
    except Exception:
        print("Max retries reached. Returning None.")
        return None

class Suggest:
    def __init__(self):
        self.lang_dict = {"ko":Ko(),
                          "ja":Ja(),
                          "en":En()}    
    
    def _requests(self,
                  targets,
                  lang,
                  service, # ['google', 'youtube']
                  num_processes : int = 30) -> dict:
        lang : LanguageBase = self.lang_dict[lang]
        targets = [SuggestApiParams(query=t, 
                                    hl=lang.hl,
                                    gl=lang.gl,
                                    expand_mode='exact',
                                    ds=service) \
                                    for t in targets]
        print(targets[0])
        with Pool(num_processes) as pool:
            result = pool.map(fetch_suggestions, targets)
        result = [res for res in result if res is not None]
        return result