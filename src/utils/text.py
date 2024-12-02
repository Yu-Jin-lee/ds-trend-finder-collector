import re
from typing import List

def contains_korean(text):
    korean_pattern = re.compile('[ㄱ-ㅎㅏ-ㅣ가-힣]+')
    return bool(korean_pattern.search(text))


def extract_initial(char) -> str:
    '''
    한국어 초성만 추출
    '''
    CHOSEONG_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    # 유니코드에서 한글 문자의 시작 코드
    BASE_CODE = 44032
    # 초성의 개수 (한글 유니코드 범위에서 초성은 19개)
    CHOSEONG_COUNT = 588
    
    # 유니코드 값에서 한글 시작 값과의 차이 계산
    if '가' <= char <= '힣':  # 한글 범위 내에 있을 때
        char_code = ord(char) - BASE_CODE
        choseong_index = char_code // CHOSEONG_COUNT
        return CHOSEONG_LIST[choseong_index]
    else:
        return char  # 한글이 아닌 문자는 그대로 반환
    
def get_initial(char):
    '''
    초성 추출
    char : 한글자 문자
    '''
    # 한글 초성 추출
    if '가' <= char <= '힣':
        cho_index = (ord(char) - ord('가')) // 588
        chosung_list = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
        return chosung_list[cho_index]
    # 한글이 아니면 그대로 반환
    return char

def extract_initial_next_target_keyword(keywords, target_keyword) -> List[str]:
    '''
    대상 키워드 다음 글자의 초성 추출
    '''
    result = []
    for keyword in keywords:
        # 대상 키워드를 기준으로 나눔
        parts = keyword.split(target_keyword)
        if len(parts) > 1 and len(parts[1]) > 0:
            # 대상 키워드 다음에 나오는 첫 글자의 초성 추출
            first_char = parts[1].strip()[0]
            result.append(get_initial(first_char))
    return result

def normalize_spaces(text):
    """문자열의 공백을 정규화하여 연속된 공백을 하나로 만듭니다."""
    return re.sub(r'\s{2,}', ' ', text)