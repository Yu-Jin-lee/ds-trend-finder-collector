class SuggestValidator:
    @staticmethod
    def contain_all_characters(search_query,suggestion) -> bool:
        ## 검색어의 모든 문자가 세제스트에 있는지 체크
        ## api에서 type이 0인 경우 영문자도 한글 자판에 따라 변경되어 있다고 판단돼서 함수 만듦
        search_query = search_query.replace(' ','').lower()
        suggestion = suggestion.replace(' ','').lower()        
        return True if len(set(search_query) - set(suggestion)) == 0 else False

    @staticmethod
    def is_unrelated_suggest(type: int) -> bool:
        ## ...[추천단어] 형식의 서제스트인지 체크 33                
        return True if type in [33] else False

    @staticmethod
    def is_spelled_out_suggest_1(type: int, subtype: list) -> bool:
        ## 관련있지만 검색어가 일부 변형된 서제스트
        return True if type in [0, 46] and 333 in subtype else False
    
    @staticmethod
    def is_spelled_out_suggest_2(type: int, subtype: list) -> bool:
        ## 관련있지만 검색어가 일부 변형된 서제스트
        return True if type in [0, 46] and 546 in subtype else False
    
    @staticmethod
    def is_spelled_out_suggest_3(type: int, subtype: list) -> bool:
        ## 관련있지만 검색어가 일부 변형된 서제스트
        return True if type in [0, 46] and 10 in subtype else False

    @staticmethod
    def is_typo_correction_suggest(type: int, subtype: list) -> bool:
        ## 오타가 자동으로 교정된 서제스트
        return True if type in [0, 46] and 13 in subtype else False

    @staticmethod
    def is_infix_suggest(type: int, subtype: list) -> bool:
        ## 검색어 사이에 단어가 추가되는 서제스트
        ## 검색어 순서 변경된 서제스트
        return True if type in [0, 46] and (7 in subtype or 8 in subtype) else False

    @staticmethod
    def is_prefix_suggest(type: int, subtype: list) -> bool:
        ## 검색어 전방에 단어가 추가되는 서제스트
        return True if type in [0, 46] and (5 in subtype) else False

    @staticmethod
    def is_suffix_suggest(type: int, subtype: list) -> bool:
        ## 검색어 후방에 단어가 추가되는 서제스트
        return True if type in [0, 46] and (512 in subtype) else False

    @staticmethod
    def is_related_low_suggest(type: int, subtype: list) -> bool:
        ## 관련성이 낮은 서제스트
        return True if type in [0, 46] and (30 in subtype) else False
    
    @staticmethod
    def is_suffix_trend_suggest(type: int, subtype: list) -> bool: # 트렌드 키워드 조건
        ## 검색어 후방에 단어가 추가되는 서제스트
        return True if type in [0, 46] and (3 in subtype) else False
    
    @staticmethod
    def is_valid_suggest(type: int, subtype: list) -> bool:
        is_valid = False
        if (SuggestValidator.is_prefix_suggest(type, subtype) or \
            SuggestValidator.is_infix_suggest(type, subtype) or \
            SuggestValidator.is_suffix_suggest(type, subtype) or \
            SuggestValidator.is_related_low_suggest(type, subtype) or
            SuggestValidator.is_suffix_trend_suggest(type, subtype)):
            is_valid = True
        if SuggestValidator.is_unrelated_suggest(type) or \
            SuggestValidator.is_spelled_out_suggest_1(type, subtype) or \
            SuggestValidator.is_spelled_out_suggest_2(type, subtype) or \
            SuggestValidator.is_spelled_out_suggest_3(type, subtype) or \
            SuggestValidator.is_typo_correction_suggest(type, subtype):
            is_valid = False
        return is_valid