import json
import pandas as pd
import sqlalchemy
from urllib.parse import quote
from typing import List
from datetime import timedelta

from utils.converter import DateConverter

class QueryDatabaseKo:
    
    schema = 'query_ko'
    db = {
        "user": 'yjlee',
        "password": quote("Ascent123!@#"),
        "host": "analysis002.dev.ascentlab.io",
        "port": 10086,
    "database": schema,
    }
    
    DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8"
    engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20, pool_recycle=3600)    
    metadata = sqlalchemy.MetaData(bind=engine)
    print('create engine')
    
    @staticmethod
    def get_connection():
        for i in range(10):
            try:
                with QueryDatabaseKo.engine.connect() as connection:
                    pass
                return QueryDatabaseKo.engine, QueryDatabaseKo.metadata
            except Exception as e:                
                #logger.warning(f'{str(e)} MYSQL 연결 재시도 {i+1}...')
                DB_URL = f"mysql+mysqlconnector://{QueryDatabaseKo.db['user']}:{QueryDatabaseKo.db['password']}@{QueryDatabaseKo.db['host']}:{QueryDatabaseKo.db['port']}/{QueryDatabaseKo.db['database']}?charset=utf8"
                QueryDatabaseKo.engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20,pool_recycle=3600)    
                QueryDatabaseKo.metadata = sqlalchemy.MetaData(bind=QueryDatabaseKo.engine)
        raise Exception('MYSQL 연결 실패')

    @staticmethod
    def get_suggest_target_keywords() -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT s FROM {QueryDatabaseKo.schema}.oi_suggest_labels;"
        suggest_target_keywords = pd.read_sql(query, con=engine)
        suggest_target_keywords = list(suggest_target_keywords['s'])
        suggest_target_keywords = [kw.replace("_", " ") for kw in suggest_target_keywords]
        engine.dispose()
        return suggest_target_keywords

    @staticmethod
    def get_target_keyword_by_user() -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f'''SELECT s as target_keyword, 
                    SUBSTRING(json_extract(properties, "$.user"), 2, LENGTH(json_extract(properties, "$.user")) - 2) as user_id,
                    SUBSTRING(json_extract(properties, "$.source[0]"), 2, LENGTH(json_extract(properties, "$.source[0]")) - 2) as source 
                    FROM {QueryDatabaseKo.schema}.oi_suggest_labels;
                '''
        target_keywords_by_user = pd.read_sql(query, con=engine)
        engine.dispose()
        return target_keywords_by_user
    
    @staticmethod
    def upsert_google_suggest_trend(args : list):
        '''
        args = [[job_id, category, info]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "google_suggest_trend"
        def _query_col_list():
            query = """
                `job_id`,
                `category`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `job_id`=VALUES(`job_id`),
                `category`=VALUES(`category`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
    
    @staticmethod
    def upsert_google_suggest_trend_2(args : list):
        '''
        args = [[job_id, category, info]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "google_suggest_trend_2"
        def _query_col_list():
            query = """
                `job_id`,
                `category`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `job_id`=VALUES(`job_id`),
                `category`=VALUES(`category`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_google_suggest_trend_target(args : list):
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "google_suggest_trend_target"
        def _query_col_list():
            query = """
                `user_id`,
                `job_id`,
                `target_keyword`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `user_id`=VALUES(`user_id`),
                `job_id`=VALUES(`job_id`),
                `target_keyword`=VALUES(`target_keyword`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  6 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()

    @staticmethod
    def get_hash_json_by_keywords(keywords:list) -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        batch_size = 100
        result = pd.DataFrame(columns = ['keyword', 'hash', 'json'])
        for i in range(0, len(keywords), batch_size):
            if len(keywords[i : i+batch_size])>1:
                query = f'''SELECT keyword, hash, json
                            FROM {QueryDatabaseKo.schema}.serp_history WHERE keyword in {tuple(keywords[i : i+batch_size])};
                         '''
            elif len(keywords[i : i+batch_size])==1:
                query = f'''SELECT keyword, hash, json
                            FROM {QueryDatabaseKo.schema}.serp_history WHERE keyword = "{keywords[0]}";
                         '''
            result = pd.concat([result, pd.read_sql(query, con=engine)], axis=0).reset_index(drop=True)
        engine.dispose()
        return result
    
    @staticmethod
    def get_hash_json_over_collected_time(kws:str, 
                                          collected_time:str # "yyyy-mm-dd"
                                         ):
        '''
        collected_time 이후에 수집된 서프만 가져오기
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        batch_size = 100
        check_serp = pd.DataFrame(columns = ['keyword'])
        for i in range(0, len(kws), batch_size):
            batch_keywords = kws[i:i+batch_size]
            if len(batch_keywords) == 1:
                query = f"SELECT keyword, json, hash, collected_time FROM {QueryDatabaseKo.schema}.serp_history WHERE keyword ='{batch_keywords[0]}' and collected_time >= '{collected_time}';"
            else:
                query = f"SELECT keyword, json, hash, collected_time FROM {QueryDatabaseKo.schema}.serp_history WHERE keyword in {tuple(batch_keywords)} and collected_time >= '{collected_time}';"
            tmp_serp = pd.read_sql(query, con = engine)
            if len(tmp_serp) > 0:
                check_serp = pd.concat([check_serp, tmp_serp], axis = 0).reset_index(drop=True)
        engine.dispose()
        return check_serp
        
    @staticmethod
    def get_transaction_url() -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT url, category FROM {QueryDatabaseKo.schema}.transaction_url"
        transaction_url = pd.read_sql(query, con=engine)
        engine.dispose()
        return transaction_url
    
    @staticmethod
    def get_contents_collection_status(urls : List[str]) -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        if len(urls) == 1:
            query = f"SELECT * FROM {QueryDatabaseKo.schema}.contents_collection_status WHERE url = '{urls[0]}';"
        else:
            query = f"SELECT * FROM {QueryDatabaseKo.schema}.contents_collection_status WHERE url in {tuple(urls)};"
        contents_collection_status = pd.read_sql(query, con=engine)
        engine.dispose()
        return contents_collection_status
    
    @staticmethod
    def upsert_contents_collection_status(args : list):
        '''
        args = [[url, publication_date, register_time, collected_time, info]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "contents_collection_status"
        def _query_col_list():
            query = """
                `url`,
                `publication_date`,
                `register_time`,
                `collected_time`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `publication_date`=VALUES(`publication_date`),
                `register_time`=VALUES(`register_time`),
                `collected_time`=VALUES(`collected_time`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """
        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def get_google_suggest_trend_category_keywords(job_id, source) -> pd.DataFrame:
        '''
        google_suggest_trend 테이블에서 job_id와 source에 해당하는 category와 keywords를 가져오는 함수
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT category, json_extract(info, '$.keywords') as keywords FROM {QueryDatabaseKo.schema}.google_suggest_trend WHERE job_id = '{job_id}' and source = '{source}';"
        suggest_trend_category_keywords = pd.read_sql(query, con=engine)
        suggest_trend_category_keywords['keywords'] = suggest_trend_category_keywords['keywords'].apply(lambda x : eval(x))
        engine.dispose()
        return suggest_trend_category_keywords
    
    @staticmethod
    def get_google_suggest_trend_table_by_job_id_source(job_id, source) -> pd.DataFrame:
        '''
        google_suggest_trend 테이블에서 job_id와 source에 해당하는 category와 keywords를 가져오는 함수
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT * FROM {QueryDatabaseKo.schema}.google_suggest_trend WHERE job_id = '{job_id}' and source = '{source}';"
        suggest_trend = pd.read_sql(query, con=engine)
        suggest_trend['info'] = suggest_trend['info'].apply(lambda x : eval(x))
        engine.dispose()
        return suggest_trend
    
    @staticmethod
    def get_google_suggest_trend_target_by_job_id_source(job_id, source) -> pd.DataFrame:
        '''
        google_suggest_trend_target 테이블에서 job_id와 source에 해당하는 row를 가져오는 함수
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT * FROM {QueryDatabaseKo.schema}.google_suggest_trend_target WHERE job_id = '{job_id}' and source = '{source}';"
        google_suggest_trend_target = pd.read_sql(query, con=engine)
        google_suggest_trend_target['info'] = google_suggest_trend_target['info'].apply(lambda x : eval(x))
        engine.dispose()
        return google_suggest_trend_target
    
    @staticmethod
    def get_google_suggest_trend_by_job_id_source(job_id, source) -> pd.DataFrame:
        '''
        google_suggest_trend 테이블에서 job_id와 source에 해당하는 row를 가져오는 함수
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT * FROM {QueryDatabaseKo.schema}.google_suggest_trend WHERE job_id = '{job_id}' and source = '{source}';"
        google_suggest_trend = pd.read_sql(query, con=engine)
        google_suggest_trend['info'] = google_suggest_trend['info'].apply(lambda x : eval(x))
        engine.dispose()
        return google_suggest_trend
    
    @staticmethod
    def upsert_contents(args : list):
        '''
        args = [[url, contents, collected_time, published_time]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "contents"
        def _query_col_list():
            query = """
                `url`,
                `contents`,
                `collected_time`,
                `published_time`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `contents`=VALUES(`contents`),
                `collected_time`=VALUES(`collected_time`),
                `published_time`=VALUES(`published_time`)
            """
            return query

        len_cols =  4 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_suggest_issue_contents(args : list):
        '''
        args = [[url, site_name, title, description, contents, collected_time, published_time]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "suggest_issue_contents"
        def _query_col_list():
            query = """
                `url`,
                `site_name`,
                `title`,
                `description`,
                `contents`,
                `collected_time`,
                `published_time`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `site_name`=VALUES(`site_name`),
                `title`=VALUES(`title`),
                `description`=VALUES(`description`),
                `contents`=VALUES(`contents`),
                `collected_time`=VALUES(`collected_time`),
                `published_time`=VALUES(`published_time`)
            """
            return query

        len_cols =  7 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_url_by_category(args : list):
        '''
        args = [[url, category]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "url_by_category"
        def _query_col_list():
            query = """
                `url`,
                `category`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `category`=VALUES(`category`)
            """
            return query

        len_cols =  2 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_url_by_keyword(args : list):
        '''
        args = [[date, keyword, rank, url]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "url_by_keyword"
        def _query_col_list():
            query = """
                `date`,
                `keyword`,
                `rank`,
                `url`
                """
            return query

        def _update_value():
            query = """
                `date`=VALUES(`date`),
                `keyword`=VALUES(`keyword`),
                `rank`=VALUES(`rank`),
                `url`=VALUES(`url`)
            """
            return query

        len_cols =  4 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def get_exist_urls_from_contents(urls : list) -> List[str]:
        '''
        입력한 url리스트 중 contents 테이블에 있는 url 리스트 반환
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        if len(urls) > 1:
            query = f"SELECT url FROM {QueryDatabaseKo.schema}.contents WHERE url in {tuple(urls)};"
        elif len(urls) == 1:
            if "'" in urls[0]:
                query = f'SELECT url FROM {QueryDatabaseKo.schema}.contents WHERE url = "{urls[0]}";'
            else:
                query = f"SELECT url FROM {QueryDatabaseKo.schema}.contents WHERE url = '{urls[0]}';"
        elif len(urls) == 0:
            return []
        exist_urls = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(exist_urls)>0:
            return list(exist_urls['url'])
        return []

    def get_urls_by_collected_time_from_contents_table(self, 
                                                       collected_time:str # yyyymmdd
                                                       ) -> List[str]:
        '''
        입력한 collected_time에 수집된 contents url 리스트 반환
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        collected_time_one_day_before = DateConverter.convert_datetime_to_str(DateConverter.convert_str_to_datetime(collected_time) + timedelta(days=1),
                                                                              "%Y%m%d")
        start_date = f"{collected_time[:4]}-{collected_time[4:6]}-{collected_time[6:8]}"
        end_date = f"{collected_time_one_day_before[:4]}-{collected_time_one_day_before[4:6]}-{collected_time_one_day_before[6:8]}"
        query = f'SELECT url FROM {QueryDatabaseKo.schema}.contents WHERE collected_time >= "{start_date}" and collected_time < "{end_date}";'
        urls = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(urls)>0:
            return list(urls['url'])
        return []
    
    def get_urls_by_collected_time_from_suggest_issue_contents_table(self, 
                                                                    collected_time:str # yyyymmdd
                                                                    ) -> List[str]:
        '''
        입력한 collected_time에 수집된 contents url 리스트 반환
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        collected_time_one_day_before = DateConverter.convert_datetime_to_str(DateConverter.convert_str_to_datetime(collected_time) + timedelta(days=1),
                                                                              "%Y%m%d")
        start_date = f"{collected_time[:4]}-{collected_time[4:6]}-{collected_time[6:8]}"
        end_date = f"{collected_time_one_day_before[:4]}-{collected_time_one_day_before[4:6]}-{collected_time_one_day_before[6:8]}"
        query = f'SELECT url FROM {QueryDatabaseKo.schema}.suggest_issue_contents WHERE collected_time >= "{start_date}" and collected_time < "{end_date}";'
        urls = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(urls)>0:
            return list(urls['url'])
        return []
    
    def get_urls_by_category_from_url_by_category_table(self, category : str) -> List[str]:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT url FROM {QueryDatabaseKo.schema}.url_by_category WHERE category='{category}';"
        result = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(result)>0:
            return list(result['url'])
        return result
    
    def get_keywords_list_by_date_from_url_by_keyword_table(self, 
                                                            date:str # yyyymmdd
                                                            ) -> List[str]:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT distinct(keyword) FROM {QueryDatabaseKo.schema}.url_by_keyword WHERE date='{date}';"
        result = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(result)>0:
            return list(result['keyword'])
        return []
    
    @staticmethod
    def get_llm_entity_topic() -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT keyword FROM {QueryDatabaseKo.schema}.llm_entity_topic;"
        llm_entity_topic = pd.read_sql(query, con=engine)
        llm_entity_topic = list(llm_entity_topic['keyword'])
        llm_entity_topic = [kw.replace("_", " ") for kw in llm_entity_topic]
        engine.dispose()
        return llm_entity_topic
    
class QueryDatabaseJa:
        
    schema = 'query_jp_ja'
    db = {
        "user": 'db',
        "password": quote("Ascent123!@#"),
        "host": "analysis004.dev.ascentlab.io",
        "port": 10086,
    "database": schema,
    }
    
    DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8"
    engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20, pool_recycle=3600)    
    metadata = sqlalchemy.MetaData(bind=engine)
    print('create engine')
    
    @staticmethod
    def get_connection():
        for i in range(10):
            try:
                with QueryDatabaseJa.engine.connect() as connection:
                    pass
                return QueryDatabaseJa.engine, QueryDatabaseJa.metadata
            except Exception as e:                
                #logger.warning(f'{str(e)} MYSQL 연결 재시도 {i+1}...')
                DB_URL = f"mysql+mysqlconnector://{QueryDatabaseJa.db['user']}:{QueryDatabaseJa.db['password']}@{QueryDatabaseJa.db['host']}:{QueryDatabaseJa.db['port']}/{QueryDatabaseJa.db['database']}?charset=utf8"
                QueryDatabaseJa.engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20,pool_recycle=3600)    
                QueryDatabaseJa.metadata = sqlalchemy.MetaData(bind=QueryDatabaseJa.engine)
        raise Exception('MYSQL 연결 실패')

    @staticmethod
    def get_suggest_target_keywords() -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f"SELECT s FROM {QueryDatabaseJa.schema}.oi_suggest_labels;"
        suggest_target_keywords = pd.read_sql(query, con=engine)
        suggest_target_keywords = list(suggest_target_keywords['s'])
        suggest_target_keywords = [kw.replace("_", " ") for kw in suggest_target_keywords]
        engine.dispose()
        return suggest_target_keywords

    @staticmethod
    def get_target_keyword_by_user() -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f'''SELECT s as target_keyword, 
                    SUBSTRING(json_extract(properties, "$.user"), 2, LENGTH(json_extract(properties, "$.user")) - 2) as user_id,
                    SUBSTRING(json_extract(properties, "$.source[0]"), 2, LENGTH(json_extract(properties, "$.source[0]")) - 2) as source 
                    FROM {QueryDatabaseJa.schema}.oi_suggest_labels;
                '''
        target_keywords_by_user = pd.read_sql(query, con=engine)
        return target_keywords_by_user
    
    @staticmethod
    def upsert_google_suggest_trend(args : list):
        '''
        args = [[job_id, category, info]]
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "google_suggest_trend"
        def _query_col_list():
            query = """
                `job_id`,
                `category`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `job_id`=VALUES(`job_id`),
                `category`=VALUES(`category`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_google_suggest_trend_target(args : list):
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "google_suggest_trend_target"
        def _query_col_list():
            query = """
                `user_id`,
                `job_id`,
                `target_keyword`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `user_id`=VALUES(`user_id`),
                `job_id`=VALUES(`job_id`),
                `target_keyword`=VALUES(`target_keyword`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  6 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()

    @staticmethod
    def get_hash_json_by_keywords(keywords : list) -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        batch_size = 100
        result = pd.DataFrame(columns = ['keyword', 'hash', 'json'])
        for i in range(0, len(keywords), batch_size):
            query = f'''SELECT keyword, hash, json
                        FROM {QueryDatabaseJa.schema}.serp_history WHERE keyword in {tuple(keywords[i : i+batch_size])};
                    '''
            result = pd.concat([result, pd.read_sql(query, con=engine)], axis=0).reset_index(drop=True)
        engine.dispose()
        return result
    
    @staticmethod
    def get_hash_json_over_collected_time(kws:str, 
                                          collected_time:str # "yyyy-mm-dd"
                                         ):
        '''
        collected_time 이후에 수집된 서프만 가져오기
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        batch_size = 100
        check_serp = pd.DataFrame(columns = ['keyword'])
        for i in range(0, len(kws), batch_size):
            batch_keywords = kws[i:i+batch_size]
            if len(batch_keywords) == 1:
                query = f"SELECT keyword, json, hash, collected_time FROM {QueryDatabaseJa.schema}.serp_history WHERE keyword ='{batch_keywords[0]}' and collected_time >= '{collected_time}';"
            else:
                query = f"SELECT keyword, json, hash, collected_time FROM {QueryDatabaseJa.schema}.serp_history WHERE keyword in {tuple(batch_keywords)} and collected_time >= '{collected_time}';"
            check_serp = pd.concat([check_serp, 
                                   pd.read_sql(query, 
                                               con = engine)
                                  ], axis = 0).reset_index(drop=True)
        engine.dispose()
        return check_serp
    
    @staticmethod
    def get_transaction_url() -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f"SELECT url, category FROM {QueryDatabaseJa.schema}.transaction_url"
        transaction_url = pd.read_sql(query, con=engine)
        engine.dispose()
        return transaction_url
    
    @staticmethod
    def upsert_contents(args : list):
        '''
        args = [[url, contents, collected_time, published_time]]
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "contents"
        def _query_col_list():
            query = """
                `url`,
                `contents`,
                `collected_time`,
                `published_time`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `contents`=VALUES(`contents`),
                `collected_time`=VALUES(`collected_time`),
                `published_time`=VALUES(`published_time`)
            """
            return query

        len_cols =  4 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
    
    @staticmethod
    def upsert_suggest_issue_contents(args : list):
        '''
        args = [[url, site_name, title, description, contents, collected_time, published_time]]
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "suggest_issue_contents"
        def _query_col_list():
            query = """
                `url`,
                `site_name`,
                `title`,
                `description`,
                `contents`,
                `collected_time`,
                `published_time`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `site_name`=VALUES(`site_name`),
                `title`=VALUES(`title`),
                `description`=VALUES(`description`),
                `contents`=VALUES(`contents`),
                `collected_time`=VALUES(`collected_time`),
                `published_time`=VALUES(`published_time`)
            """
            return query

        len_cols =  7 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
         
    @staticmethod
    def upsert_url_by_keyword(args : list):
        '''
        args = [[date, keyword, rank, url]]
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "url_by_keyword"
        def _query_col_list():
            query = """
                `date`,
                `keyword`,
                `rank`,
                `url`
                """
            return query

        def _update_value():
            query = """
                `date`=VALUES(`date`),
                `keyword`=VALUES(`keyword`),
                `rank`=VALUES(`rank`),
                `url`=VALUES(`url`)
            """
            return query

        len_cols =  4 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
    
    @staticmethod
    def upsert_url_by_category(args : list):
        '''
        args = [[url, category]]
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "url_by_category"
        def _query_col_list():
            query = """
                `url`,
                `category`
                """
            return query

        def _update_value():
            query = """
                `url`=VALUES(`url`),
                `category`=VALUES(`category`)
            """
            return query

        len_cols =  2 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
         
    @staticmethod
    def get_exist_urls_from_contents(urls : list) -> List[str]:
        '''
        입력한 url리스트 중 contents 테이블에 있는 url 리스트 반환
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        if len(urls) > 1:
            query = f"SELECT url FROM {QueryDatabaseJa.schema}.contents WHERE url in {tuple(urls)};"
        elif len(urls) == 1:
            if "'" in urls[0]:
                query = f'SELECT url FROM {QueryDatabaseJa.schema}.contents WHERE url = "{urls[0]}";'
            else:
                query = f"SELECT url FROM {QueryDatabaseJa.schema}.contents WHERE url = '{urls[0]}';"
        elif len(urls) == 0:
            return []
        exist_urls = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(exist_urls)>0:
            return list(exist_urls['url'])
        return []
    
    def get_urls_by_collected_time_from_contents_table(self, 
                                                       collected_time:str # yyyymmdd
                                                       ) -> List[str]:
        '''
        입력한 collected_time에 수집된 contents url 리스트 반환
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        collected_time_one_day_before = DateConverter.convert_datetime_to_str(DateConverter.convert_str_to_datetime(collected_time) + timedelta(days=1),
                                                                              "%Y%m%d")
        start_date = f"{collected_time[:4]}-{collected_time[4:6]}-{collected_time[6:8]}"
        end_date = f"{collected_time_one_day_before[:4]}-{collected_time_one_day_before[4:6]}-{collected_time_one_day_before[6:8]}"
        query = f'SELECT url FROM {QueryDatabaseJa.schema}.contents WHERE collected_time >= "{start_date}" and collected_time < "{end_date}";'
        urls = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(urls)>0:
            return list(urls['url'])
        return []
    
    def get_urls_by_collected_time_from_suggest_issue_contents_table(self, 
                                                                    collected_time:str # yyyymmdd
                                                                    ) -> List[str]:
        '''
        입력한 collected_time에 수집된 contents url 리스트 반환
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        collected_time_one_day_before = DateConverter.convert_datetime_to_str(DateConverter.convert_str_to_datetime(collected_time) + timedelta(days=1),
                                                                              "%Y%m%d")
        start_date = f"{collected_time[:4]}-{collected_time[4:6]}-{collected_time[6:8]}"
        end_date = f"{collected_time_one_day_before[:4]}-{collected_time_one_day_before[4:6]}-{collected_time_one_day_before[6:8]}"
        query = f'SELECT url FROM {QueryDatabaseJa.schema}.suggest_issue_contents WHERE collected_time >= "{start_date}" and collected_time < "{end_date}";'
        urls = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(urls)>0:
            return list(urls['url'])
        return []
    
    def get_urls_by_category_from_url_by_category_table(self, category : str) -> List[str]:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f"SELECT url FROM {QueryDatabaseJa.schema}.url_by_category WHERE category='{category}';"
        result = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(result)>0:
            return list(result['url'])
        return result
    
    def get_keywords_list_by_date_from_url_by_keyword_table(self, 
                                                            date:str # yyyymmdd
                                                            ) -> List[str]:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f"SELECT distinct(keyword) FROM {QueryDatabaseJa.schema}.url_by_keyword WHERE date='{date}';"
        result = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(result)>0:
            return list(result['keyword'])
        return []
    
    @staticmethod
    def get_llm_entity_topic() -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f"SELECT keyword FROM {QueryDatabaseJa.schema}.llm_entity_topic;"
        llm_entity_topic = pd.read_sql(query, con=engine)
        llm_entity_topic = list(llm_entity_topic['keyword'])
        llm_entity_topic = [kw.replace("_", " ") for kw in llm_entity_topic]
        engine.dispose()
        return llm_entity_topic

class QueryDatabaseEn:
        
    schema = 'query_us_en'
    db = {
        "user": 'db',
        "password": quote("Ascent123!@#"),
        "host": "analysis004.dev.ascentlab.io",
        "port": 10086,
    "database": schema,
    }
    
    DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8"
    engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20, pool_recycle=3600)    
    metadata = sqlalchemy.MetaData(bind=engine)
    print('create engine')
    
    @staticmethod
    def get_connection():
        for i in range(10):
            try:
                with QueryDatabaseEn.engine.connect() as connection:
                    pass
                return QueryDatabaseEn.engine, QueryDatabaseEn.metadata
            except Exception as e:                
                #logger.warning(f'{str(e)} MYSQL 연결 재시도 {i+1}...')
                DB_URL = f"mysql+mysqlconnector://{QueryDatabaseEn.db['user']}:{QueryDatabaseEn.db['password']}@{QueryDatabaseEn.db['host']}:{QueryDatabaseEn.db['port']}/{QueryDatabaseEn.db['database']}?charset=utf8"
                QueryDatabaseEn.engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20,pool_recycle=3600)    
                QueryDatabaseEn.metadata = sqlalchemy.MetaData(bind=QueryDatabaseEn.engine)
        raise Exception('MYSQL 연결 실패')
    
    @staticmethod
    def get_suggest_target_keywords() -> pd.DataFrame:
        engine, metadata = QueryDatabaseEn.get_connection()
        query = f"SELECT s FROM {QueryDatabaseEn.schema}.oi_suggest_labels;"
        suggest_target_keywords = pd.read_sql(query, con=engine)
        suggest_target_keywords = list(suggest_target_keywords['s'])
        suggest_target_keywords = [kw.replace("_", " ") for kw in suggest_target_keywords]
        engine.dispose()
        return suggest_target_keywords
    
    @staticmethod
    def get_target_keyword_by_user() -> pd.DataFrame:
        engine, metadata = QueryDatabaseEn.get_connection()
        query = f'''SELECT s as target_keyword, 
                    SUBSTRING(json_extract(properties, "$.user"), 2, LENGTH(json_extract(properties, "$.user")) - 2) as user_id,
                    SUBSTRING(json_extract(properties, "$.source[0]"), 2, LENGTH(json_extract(properties, "$.source[0]")) - 2) as source 
                    FROM {QueryDatabaseEn.schema}.oi_suggest_labels;
                '''
        target_keywords_by_user = pd.read_sql(query, con=engine)
        engine.dispose()
        return target_keywords_by_user
    
    @staticmethod
    def upsert_google_suggest_trend(args : list):
        '''
        args = [[job_id, category, info]]
        '''
        engine, metadata = QueryDatabaseEn.get_connection()
        table_name = "google_suggest_trend"
        def _query_col_list():
            query = """
                `job_id`,
                `category`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `job_id`=VALUES(`job_id`),
                `category`=VALUES(`category`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_google_suggest_trend_target(args : list):
        engine, metadata = QueryDatabaseEn.get_connection()
        table_name = "google_suggest_trend_target"
        def _query_col_list():
            query = """
                `user_id`,
                `job_id`,
                `target_keyword`,
                `source`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `user_id`=VALUES(`user_id`),
                `job_id`=VALUES(`job_id`),
                `target_keyword`=VALUES(`target_keyword`),
                `source`=VALUES(`source`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  6 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def get_google_suggest_trend_target_by_job_id_source(job_id, source) -> pd.DataFrame:
        '''
        google_suggest_trend_target 테이블에서 job_id와 source에 해당하는 row를 가져오는 함수
        '''
        engine, metadata = QueryDatabaseEn.get_connection()
        query = f"SELECT * FROM {QueryDatabaseEn.schema}.google_suggest_trend_target WHERE job_id = '{job_id}' and source = '{source}';"
        google_suggest_trend_target = pd.read_sql(query, con=engine)
        google_suggest_trend_target['info'] = google_suggest_trend_target['info'].apply(lambda x : eval(x))
        engine.dispose()
        return google_suggest_trend_target
    
    def get_keywords_list_by_date_from_url_by_keyword_table(self, 
                                                            date:str # yyyymmdd
                                                            ) -> List[str]:
        engine, metadata = QueryDatabaseEn.get_connection()
        query = f"SELECT distinct(keyword) FROM {QueryDatabaseEn.schema}.url_by_keyword WHERE date='{date}';"
        result = pd.read_sql(query, con=engine)
        engine.dispose()
        if len(result)>0:
            return list(result['keyword'])
        return []
    
    @staticmethod
    def get_llm_entity_topic() -> pd.DataFrame:
        engine, metadata = QueryDatabaseEn.get_connection()
        query = f"SELECT keyword FROM {QueryDatabaseEn.schema}.llm_entity_topic;"

        with engine.connect() as connection:
            llm_entity_topic = pd.read_sql(query, con=connection)
        llm_entity_topic = list(llm_entity_topic['keyword'])
        llm_entity_topic = [kw.replace("_", " ") for kw in llm_entity_topic]
        engine.dispose()
        return llm_entity_topic
        
def convert_dict_to_json_insert_format(dictionary_value : dict):
    '''
    파이썬 딕셔너리 값을 JSON 데이터 타입인 필드에 값을 넣을 때 알맞은 형식으로 변환해주는 함수
    '''
    return json.dumps(dictionary_value, ensure_ascii=False)

def check_length(string, max_length):
    return len(string) <= max_length
    

