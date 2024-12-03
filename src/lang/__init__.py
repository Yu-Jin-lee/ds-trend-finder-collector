from lang.ko.url import urls_by_type_ko
from lang.ja.url import urls_by_type_ja
from lang.ko.url import tokens_by_type_ko
from lang.ja.url import tokens_by_type_ja
from lang.ko.url import domain_by_type_ko
from lang.ja.url import domain_by_type_ja
from lang.ko.url import except_tokens_for_transaction_url_ko, except_url_for_transaction_url_ko
from lang.ja.url import except_tokens_for_transaction_url_ja, except_url_for_transaction_url_ja

from lang.ko.ko import Ko
from lang.ja.ja import Ja
from lang.en.en import En
from lang.lang_base import LanguageBase

from lang.en.filtering import filter_en_valid_trend_keyword, filter_en_valid_token_count

from utils.db import QueryDatabaseJa, QueryDatabaseKo, QueryDatabaseEn


def get_urls_by_type_and_language(language : str, 
                                  type : str) -> list:
    if language == "ko":
        return urls_by_type_ko[type]
    elif language == "ja":
        return urls_by_type_ja[type]
    
def get_tokens_by_type_and_language(language : str,
                                    type : str) -> list:
    if language == "ko":
        return tokens_by_type_ko[type]
    elif language == "ja":
        return tokens_by_type_ja[type]
    
def get_domain_by_type_and_language(language : str,
                                    type : str) -> list:
    if language == "ko":
        return domain_by_type_ko[type]
    elif language == "ja":
        return domain_by_type_ja[type]
    
def get_except_tokens_for_transaction_url_by_language(language : str):
    if language == "ko":
        return except_tokens_for_transaction_url_ko
    if language == "ja":
        return except_tokens_for_transaction_url_ja
    
def get_except_urls_for_transaction_url_by_language(language : str):
    if language == "ko":
        return except_url_for_transaction_url_ko
    if language == "ja":
        return except_url_for_transaction_url_ja
    
def get_querydatabase_by_language(language):
    if language == 'ko':
        return QueryDatabaseKo()
    
    elif language == 'ja':
        return QueryDatabaseJa()
    
    elif language == 'en':
        return QueryDatabaseEn()
    
def get_language(language:str):
    if language == 'ko':
        return Ko()
    
    elif language == 'ja':
        return Ja()
    
    elif language == 'en':
        return En()