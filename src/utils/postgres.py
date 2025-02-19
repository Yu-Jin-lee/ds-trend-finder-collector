import psycopg2
from psycopg2 import OperationalError
from typing import List, Tuple
import pandas as pd

class PostGresBase:
    schema_name: str = ""

    @staticmethod
    def connection():
        '''
        PostGresDB에 연결
        '''
        try:
            conn = psycopg2.connect(
                host="10.10.210.100",
                database="postgres",
                user="yjlee",
                password="yjlee"
            )
            return conn
        except OperationalError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None

    @classmethod
    def insert_to_task_history(cls,
                               args: List[Tuple], 
                                insert_type: str = "ignore" # "update" or "ignore"
                                ):
        '''
        task_history 테이블에 정보 insert
        '''
        table_name = "task_history"
        conn = cls.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return

        try:
            cur = conn.cursor()
            batch_size = 100
            for i in range(0, len(args), batch_size):
                if insert_type == "ignore":
                    insert_query = f"""
                                    INSERT INTO {cls.schema_name}.{table_name} (project_name, task_name, job_id, status, started_at, completed_at, info)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (project_name, task_name, job_id) DO NOTHING;
                                    """
                else:
                    insert_query = f"""
                                    INSERT INTO {cls.schema_name}.{table_name} (project_name, task_name, job_id, status, started_at, completed_at, info)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (project_name, task_name, job_id) DO UPDATE
                                    SET status = EXCLUDED.status,
                                        started_at = EXCLUDED.started_at,
                                        completed_at = EXCLUDED.completed_at,
                                        info = EXCLUDED.info;
                                    """
                insert_data = args[i:i+batch_size]
                cur.executemany(insert_query, insert_data)
                conn.commit()
            cur.close()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            conn.close()
    
    @classmethod
    def insert_to_user_interest(cls,
                                args: List[Tuple], 
                                insert_type: str = "ignore" # "update" or "ignore"
                                ):
        '''
        user_interest 테이블에 정보 insert
        '''
        table_name = "user_interest"
        conn = cls.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return

        try:
            cur = conn.cursor()
            batch_size = 100
            for i in range(0, len(args), batch_size):
                if insert_type == "ignore":
                    insert_query = f"""
                                    INSERT INTO {cls.schema_name}.{table_name} (user_id, interest, description, vector, collected_time)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (user_id, interest) DO NOTHING;
                                    """
                else:
                    insert_query = f"""
                                    INSERT INTO {cls.schema_name}.{table_name} (user_id, interest, description, vector, collected_time)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (user_id, interest) DO UPDATE
                                    SET description = EXCLUDED.description,
                                        vector = EXCLUDED.vector,
                                        collected_time = EXCLUDED.collected_time;
                                    """
                insert_data = args[i:i+batch_size]
                cur.executemany(insert_query, insert_data)
                conn.commit()
            cur.close()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            conn.close()

    @classmethod
    def get_description_vector_from_user_interest(cls,
                                                  user:str,
                                                  interest:str) -> List[Tuple]:
        '''
        user_interest 테이블에서 description과 vector를 가져오는 함수
        '''
        table_name = "user_interest"
        conn = cls.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return
        result = []
        try:
            cur = conn.cursor()
            query = f"SELECT description, vector FROM {cls.schema_name}.{table_name} WHERE user_id = '{user}' and interest = '{interest}';"
            cur.execute(query)
            rows = cur.fetchall()

            for row in rows:
                result.append(row)
            cur.close()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            conn.close()
        return result
    
    @classmethod
    def get_topics_from_llm_entity_topic(cls) -> List[str]:
        '''
        llm_entity_topic 테이블에서 모든 topic 리스트를 가져오는 함수
        '''
        table_name = "llm_entity_topic"
        conn = cls.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return
        result = []
        try:
            cur = conn.cursor()
            query = f"SELECT topic FROM {cls.schema_name}.{table_name};"
            cur.execute(query)
            rows = cur.fetchall()

            for row in rows:
                result.append(row[0])
            cur.close()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            conn.close()
        return result
    
    @classmethod
    def get_info_from_task_history_by_task_name_date(cls, 
                                                     task_name:str, 
                                                     date:str # yyyymmdd
                                                     ) -> pd.DataFrame:
        conn = cls.connection()
        if conn is None:
            print("Connection failed. Exiting the operation.")
            return

        cur = conn.cursor()
        cur.execute(f"SELECT task_name, job_id, status, info FROM {cls.schema_name}.task_history WHERE task_name like '{task_name}%' and job_id like '{date}%';")
        results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(results, columns=colnames)
        cur.close()
        conn.close()

        return df

    @classmethod
    def get_ne_topics_from_daily_topic(cls, 
                                       start_date:str, 
                                       end_date:str, 
                                       first_seen_cnt:int) -> List[str]:
        '''
        daily_topic 테이블에서 일주일안에 등록되지 않은 topic 리스트를 가져오는 함수
        '''
        table_name = "daily_topic"
        conn = cls.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return
        result = []
        try:
            cur = conn.cursor()
            query = f"SELECT distinct topic FROM {cls.schema_name}.{table_name} dt WHERE job_date between '{start_date}' and '{end_date}' and topic_type ='ne' and first_seen_cnt > {first_seen_cnt};"
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                result.append(row[0])
            cur.close()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            conn.close()
        result = list(set(result))
        return result
    
class PostGresKo(PostGresBase):
    schema_name: str = "public"


class PostGresJa(PostGresBase):
    schema_name: str = "query_jp_ja"


class PostGresEn(PostGresBase):
    schema_name: str = "query_us_en"

def get_post_gres(lang:str):
    if lang == "ko":
        return PostGresKo
    elif lang == "ja":
        return PostGresJa
    elif lang == "en":
        return PostGresEn