from typing import List, Tuple, Union
from datetime import datetime
from validator.suggest_validator import SuggestValidator

def cnt_valid_suggest(suggestions:List[dict], # 서제스트 결과
                      input_text:str, # 실제 입력한 키워드
                      log:bool=False,
                      return_result:bool=False) -> Union[int, Tuple[int, List[str]]]:
    '''
    기본 서제스트 결과에서 유효한 서제스트 개수를 반환
    '''
    try:
        valid_suggests = []
        for suggestion in suggestions:
            if SuggestValidator.is_valid_suggest(suggestion['suggest_type'], suggestion['suggest_subtypes']):
                if suggestion['text'].replace(' ', '').startswith(input_text.replace(' ', '')): # 입력한 키워드로 시작하는 서제스트일 경우만 추출
                    valid_suggests.append(suggestion['text'])
                    if log:
                        print(suggestion['text'], suggestion['suggest_type'], suggestion['suggest_subtypes'])
        if return_result:
            return len(valid_suggests), valid_suggests
        return len(valid_suggests)
    except Exception as e:
        if log:
            print(f"[{datetime.now()}] Error from cnt_valid_suggest: (target_keyword:{input_text} | error msg : {e}")
        if return_result:
            return 0, []
        return 0
    
def is_trend_keyword(text : str,
                     suggest_type : int, 
                     suggest_subtypes : list,
                     target_kw : str = None) -> bool:
    if SuggestValidator.is_suffix_trend_suggest(suggest_type, suggest_subtypes):
        if target_kw != None: # 타겟 키워드 있으면
            if text.startswith(target_kw + " "): # <타겟 키워드 + " "> 로 시작하는 경우만 추출
                return True
        else:
            return True
    return False