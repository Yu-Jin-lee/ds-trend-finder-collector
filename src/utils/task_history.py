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
        if lang == "ja":
            return 'query_jp_ja'
        else:
            return 'public'
        
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

    def set_task_completed(self):
        """
        Sets the task status to 'Completed' and updates the completed_at field.
        """
        try:
            connection = self.connect_to_db()
            with connection.cursor() as cursor:
                query = sql.SQL(f"""
                    UPDATE {self.schema}.{self.table_name}
                    SET status = %s, completed_at = %s
                    WHERE project_name = %s AND task_name = %s AND job_id = %s
                """)
                cursor.execute(
                    query,
                    (TaskStatus.COMPLETED.value, datetime.now(), self.project_name, self.task_name, self.job_id)
                )
                print(f"[{datetime.now()}] Task {self.task_name} (job_id: {self.job_id}) is now completed.")
            connection.close()
        except Exception as e:
            print(f"[{datetime.now()}] An error occurred while updating the task to completed: {e}")

    def set_task_error(self, error_msg:str=""):
        """
        Sets the task status to 'Error' and updates the completed_at field along with error info.
        """
        try:
            connection = self.connect_to_db()
            with connection.cursor() as cursor:
                query = sql.SQL(f"""
                    UPDATE {self.schema}.{self.table_name}
                    SET status = %s, completed_at = %s, info = %s
                    WHERE project_name = %s AND task_name = %s AND job_id = %s
                """)
                cursor.execute(
                    query,
                    (TaskStatus.FAILED.value, datetime.now(), json.dumps({"error_msg":error_msg}, ensure_ascii=False), self.project_name, self.task_name, self.job_id)
                )
                print(f"[{datetime.now()}] Task {self.task_name} (job_id: {self.job_id}) ended with an error.")
            connection.close()
        except Exception as e:
            print(f"[{datetime.now()}] An error occurred while updating the task to error status: {e}")