import psycopg2
from psycopg2 import OperationalError
from typing import List, Tuple

class PostGres:
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
    
    @staticmethod
    def insert_to_task_history(args: List[Tuple], 
                                insert_type: str = "ignore" # "update" or "ignore"
                                ):
        '''
        task_history 테이블에 정보 insert
        '''
        table_name = "task_history"
        conn = PostGres.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return

        try:
            cur = conn.cursor()
            batch_size = 100
            for i in range(0, len(args), batch_size):
                if insert_type == "ignore":
                    insert_query = f"""
                                    INSERT INTO {table_name} (project_name, task_name, job_id, status, started_at, completed_at, info)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (project_name, task_name, job_id) DO NOTHING;
                                    """
                else:
                    insert_query = f"""
                                    INSERT INTO {table_name} (project_name, task_name, job_id, status, started_at, completed_at, info)
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

    @staticmethod
    def insert_to_user_interest(args: List[Tuple], 
                                insert_type: str = "ignore" # "update" or "ignore"
                                ):
        '''
        user_interest 테이블에 정보 insert
        '''
        table_name = "user_interest"
        conn = PostGres.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return

        try:
            cur = conn.cursor()
            batch_size = 100
            for i in range(0, len(args), batch_size):
                if insert_type == "ignore":
                    insert_query = f"""
                                    INSERT INTO {table_name} (user_id, interest, description, vector, collected_time)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (user_id, interest) DO NOTHING;
                                    """
                else:
                    insert_query = f"""
                                    INSERT INTO {table_name} (user_id, interest, description, vector, collected_time)
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
    
    @staticmethod
    def get_description_vector_from_user_interest(user:str, interest:str) -> List[Tuple]:
        '''
        user_interest 테이블에서 description과 vector를 가져오는 함수
        '''
        table_name = "user_interest"
        conn = PostGres.connection()
        if conn is None:
            print("Connection failed. Exiting the insert operation.")
            return
        result = []
        try:
            cur = conn.cursor()
            query = f"SELECT description, vector FROM {table_name} WHERE user_id = '{user}' and interest = '{interest}';"
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