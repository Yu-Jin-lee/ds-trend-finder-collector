import re

def filter_en_valid_trend_keyword(keyword: str) -> bool:
    """
    필터링 조건에 맞는 키워드인지 확인하는 함수.
    
    조건:
    - 영어, 숫자, 공백, 특수기호만 포함된 키워드만 True 반환
    - 영어 알파벳 또는 숫자 중 1자 이상은 필수로 포함되어야 함
    
    Parameters:
    keyword (str): 검사할 키워드 문자열
    
    Returns:
    bool: 키워드가 조건에 맞으면 True, 그렇지 않으면 False
    
    사용 예시:
    >>> filter_en_valid_trend_keyword("hello 123!")
    True  # 영어, 숫자, 특수기호 포함
    
    >>> filter_en_valid_trend_keyword("hello world")
    True  # 영어만 포함
    
    >>> filter_en_valid_trend_keyword("12345")
    True  # 숫자만 포함
    
    >>> filter_en_valid_trend_keyword("!@#")
    False  # 영어, 숫자 미포함
    
    >>> filter_en_valid_trend_keyword("hello@world")
    True  # 영어, 특수기호 포함
    """
    
    # 영어 또는 숫자가 포함되었는지 체크
    if not re.search(r'[A-Za-z0-9]', keyword):
        return False
    
    # 영어, 숫자, 공백, 특수기호만 포함된 키워드인지 확인
    if re.match("^[A-Za-z0-9\s!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~]+$", keyword):
        return True
    return False

def filter_en_valid_token_count(text: str, valid_cnt: int = 20) -> bool:
    """
    입력 문자열의 토큰(띄어쓰기 기준) 개수가 valid_cnt개 이하인지 확인하는 함수.
    
    조건:
    - 토큰 개수가 valid_cnt개를 넘으면 False 반환
    - 토큰 개수가 valid_cnt개 이하이면 True 반환
    
    Parameters:
    text (str): 검사할 문자열
    
    Returns:
    bool: 토큰 개수가 valid_cnt개 이하이면 True, 그렇지 않으면 False
    
    사용 예시:
    >>> filter_en_valid_token_count("This is a sample text with less than twenty tokens.")
    True  # 토큰 개수 valid_cnt개 이하
    
    >>> filter_en_valid_token_count("Word " * 21)
    False  # 토큰 개수 valid_cnt개 초과
    """
    # 문자열을 공백 기준으로 분리하고 토큰 개수 확인
    token_count = len(text.split())
    return token_count <= valid_cnt