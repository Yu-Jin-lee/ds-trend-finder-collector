import json
import psycopg2
from enum import Enum
from psycopg2 import sql
from datetime import datetime
from dataclasses import dataclass

class TaskStatus(Enum):
    START = "Start"
    IN_PROGRESS = "In Progress"
    COMPLETED = "Completed"
    FAILED = "Failed"

class TaskHistory:
    def __init__(self, db_config:dict, project_name:str, task_name:str, job_id, lang:str):
        """
        Initializes the database connection and sets the project, task, and job ID.
        :param db_config: Dictionary containing database configuration details.
        :param project_name: Name of the project (primary key).
        :param task_name: Name of the task (primary key).
        :param job_id: Unique job identifier (primary key).
        """
        self.db_config = db_config
        self.project_name = project_name
        self.task_name = task_name
        self.job_id = job_id
        self.lang = lang
        self.schema = self.set_schema(lang)
        self.table_name = "task_history"

    def set_schema(self, lang:str):
        if lang == "ko":
            return 'public'
        if lang == "ja":
            return 'query_jp_ja'
        if lang == "en":
            return 'query_us_en'
        else:
            print(f"[{datetime.now()}] {lang} 해당 국가의 스키마가 존재하지 않습니다. (TaskHistory)")
            raise ValueError(f"{lang} 해당 국가의 스키마가 존재하지 않습니다. (TaskHistory)")
        
    def connect_to_db(self):
        con = psycopg2.connect(
            host=self.db_config["host"],
            database=self.db_config["database"],
            user=self.db_config["user"],
            password=self.db_config["password"]
        )
        con.autocommit = True
        return con
    
    def set_task_start(self):
        """
        Sets the task status to 'In Progress' and updates the started_at field.
        """
        try:
            connection = self.connect_to_db()
            with connection.cursor() as cursor:
                query = sql.SQL(f"""
                    INSERT INTO {self.schema}.{self.table_name} (project_name, task_name, job_id, status, started_at)
                    VALUES (%s, %s, %s, %s, %s)
                """)
                cursor.execute(
                    query,
                    (self.project_name, self.task_name, self.job_id, TaskStatus.START.value, datetime.now())
                )
                print(f"[{datetime.now()}] Task {self.task_name} (job_id: {self.job_id}) is now in progress.")
            connection.close()
        except Exception as e:
            print(f"[{datetime.now()}] An error occurred while updating the task to in progress: {e}")

    def set_task_in_progress(self):
        """
        Sets the task status to 'In Progress' and updates the started_at field.
        """
        try:
            connection = self.connect_to_db()
            with connection.cursor() as cursor:
                query = sql.SQL(f"""
                    UPDATE {self.schema}.{self.table_name}
                    SET status = %s, started_at = %s
                    WHERE project_name = %s AND task_name = %s AND job_id = %s
                """)
                cursor.execute(
                    query,
                    (TaskStatus.IN_PROGRESS.value, datetime.now(), self.project_name, self.task_name, self.job_id)
                )
                print(f"[{datetime.now()}] Task {self.task_name} (job_id: {self.job_id}) is now in progress.")
            connection.close()
        except Exception as e:
            print(f"[{datetime.now()}] An error occurred while updating the task to in progress: {e}")

    def set_task_completed(self, additional_info: dict = None):
        """
        Sets the task status to 'Completed' and updates the completed_at field.
        If additional_info is provided, it merges with the existing info field.
        :param additional_info: Dictionary containing additional information to update.
        """
        # try:
        connection = self.connect_to_db()
        with connection.cursor() as cursor:
            # Fetch the current info field
            query_fetch = sql.SQL(f"""
                SELECT info
                FROM {self.schema}.{self.table_name}
                WHERE project_name = %s AND task_name = %s AND job_id = %s
            """)
            cursor.execute(query_fetch, (self.project_name, self.task_name, self.job_id))
            result = cursor.fetchone()
            print(f"result : {result[0]} ({type(result[0])})")
            # Initialize existing_info as an empty dict if the field is NULL
            existing_info = {} if result is None or result[0] is None else result[0]
            print(f"existing_info : {existing_info}")

            # Merge existing info with additional_info
            if additional_info:
                existing_info.update(additional_info)

            # Update the task status and info field
            query_update = sql.SQL(f"""
                UPDATE {self.schema}.{self.table_name}
                SET status = %s, completed_at = %s, info = %s
                WHERE project_name = %s AND task_name = %s AND job_id = %s
            """)
            cursor.execute(
                query_update,
                (
                    TaskStatus.COMPLETED.value,
                    datetime.now(),
                    json.dumps(existing_info, ensure_ascii=False),
                    self.project_name,
                    self.task_name,
                    self.job_id,
                )
            )
            print(f"[{datetime.now()}] Task {self.task_name} (job_id: {self.job_id}) is now completed with updated info.")
        connection.close()
        # except Exception as e:
        #     print(f"[{datetime.now()}] An error occurred while updating the task to completed: {e}")

    def set_task_error(self, error_msg: str = ""):
        """
        Sets the task status to 'Error' and updates the completed_at field along with error info.
        If the info field already has data, it adds the error_msg key to the existing dictionary.
        :param error_msg: Error message to be added to the info field.
        """
        try:
            connection = self.connect_to_db()
            with connection.cursor() as cursor:
                # Fetch the current info field
                query_fetch = sql.SQL(f"""
                    SELECT info
                    FROM {self.schema}.{self.table_name}
                    WHERE project_name = %s AND task_name = %s AND job_id = %s
                """)
                cursor.execute(query_fetch, (self.project_name, self.task_name, self.job_id))
                result = cursor.fetchone()

                # Initialize existing_info as an empty dict if the field is NULL
                existing_info = {} if result is None or result[0] is None else result[0]

                # Add error_msg to the existing info
                existing_info["error_msg"] = error_msg

                # Update the task status and info field
                query_update = sql.SQL(f"""
                    UPDATE {self.schema}.{self.table_name}
                    SET status = %s, completed_at = %s, info = %s
                    WHERE project_name = %s AND task_name = %s AND job_id = %s
                """)
                cursor.execute(
                    query_update,
                    (
                        TaskStatus.FAILED.value,
                        datetime.now(),
                        json.dumps(existing_info, ensure_ascii=False),
                        self.project_name,
                        self.task_name,
                        self.job_id,
                    )
                )
                print(f"[{datetime.now()}] Task {self.task_name} (job_id: {self.job_id}) ended with an error and updated info.")
            connection.close()
        except Exception as e:
            print(f"[{datetime.now()}] An error occurred while updating the task to error status: {e}")