from typing import List
from itertools import chain

def combine_dictionary(dict_list : List[dict]) -> dict:
    # 결과를 저장할 빈 딕셔너리
    result_dict = {}

    # 딕셔너리 리스트를 순회하면서 합치기
    for d in dict_list:
        for key, value in d.items():
            if key in result_dict:
                result_dict[key].extend(value)
            else:
                result_dict[key] = value

    return result_dict

def flatten_nested_list(nested_list:List[list]) -> list:
    '''
    이중 리스트를 풀어주는 함수
    '''
    return list(chain(*nested_list))

def remove_duplicates_from_new_keywords(already_keywords:set, new_keywords:set) -> set:
    '''
    이미 존재하는 키워드 셋(already_keywords)과 새로운 키워드 셋(new_keywords)을 비교하여
    중복된 키워드를 제거한 새로운 키워드 셋을 반환합니다. 
    두 셋의 키워드에서 띄어쓰기를 제거하여 동일한 키워드를 식별한 후, 
    중복되지 않은 키워드만 원래 형태로 반환합니다.

    Parameters:
    - already_keywords (set): 중복을 검사할 기존의 키워드 셋.
    - new_keywords (set): 중복을 제거하고자 하는 새로운 키워드 셋.

    Returns:
    - set: 중복되지 않은 원본 형태의 새로운 키워드 셋.
    '''
    # 띄어쓰기를 제거한 already_keywords와 new_keywords를 각각 매핑
    already_keywords_normalized = {keyword.replace(" ", ""): keyword for keyword in already_keywords}
    new_keywords_normalized = {keyword.replace(" ", ""): keyword for keyword in new_keywords}

    # 중복 키워드를 제거하고 원본 키워드로 복원
    filtered_keywords = {new_keywords_normalized[key] for key in new_keywords_normalized if key not in already_keywords_normalized}
    
    return filtered_keywords

class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word: str):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True

    def starts_with(self, prefix: str) -> List[str]:
        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]
        
        # BFS or DFS to find all words with this prefix
        results = []
        self._dfs_with_prefix(node, prefix, results)
        return results

    def _dfs_with_prefix(self, node: TrieNode, prefix: str, results: List[str]):
        if node.is_end_of_word:
            results.append(prefix)
        for char, next_node in node.children.items():
            self._dfs_with_prefix(next_node, prefix + char, results)