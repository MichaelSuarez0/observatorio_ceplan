import sqlite3
import os
from correos_automaticos.classes.models import AttachmentLog
from src.models.queries import FichaQueries, VistasQueries

script_dir = os.path.dirname(__file__)
logs_dir = os.path.join(script_dir, "..", "logs")
json_path = os.path.join(logs_dir, "attachment_log.json")

db_path = os.path.join(logs_dir, "attachment_log.db")

# TODO: IDEAS FOR IMPROVING VALIDATION
# class LogEntry(BaseModel):
#     author: str
#     new_name: str
#     original_name: str
#     path: str
#     sharepoint_uploaded: int = Field(0, ge=0, le=1)  # Only 0 or 1 allowed


# TODO: Create abstract class
class DBManager:
    def __init__(self):
        self.db = "log.db"
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.queries = VistasQueries()
    
    def create_table(self):
        self.cursor.execute(self.queries.create_table) 

    def insert_log(self, data: list[AttachmentLog]):
        """Insert a log entry"""
        with self.connect() as conn:
            for details in data:
                data: AttachmentLog
                self.cursor.execute(self.queries.insert_query, (
                    details.author,
                    details.new_name,
                    details.original_name,
                    details.path,
                    details.sharepoint_uploaded
                ))
            self.conn.commit()


#CHAT GPT METHOD:

class DatabaseManager:
    def __init__(self, db_name: str = "example.db"):
        self.db_name = db_name
        self.queries = VistasQueries()
        self.cursor = None

    def connect(self):
        """Connect to the SQLite database"""
        return sqlite3.connect(self.db_name)
    
    def create_table(self):
        """Create the log table"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(self.queries.create_table)
            conn.commit()
    
    # def insert_log(self, data: list[AttachmentLog]):
    #     """Insert a log entry"""
    #     with self.connect() as conn:
    #         for details in data:
    #             cursor = conn.cursor()
    #             cursor.execute(self.queries.insert, (dict.author, new_name, original_name, path, sharepoint_uploaded))
    #             conn.commit()

    def fetch_all_logs(self):
        """Retrieve all logs"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(self.queries.select_all)
            return cursor.fetchall()

