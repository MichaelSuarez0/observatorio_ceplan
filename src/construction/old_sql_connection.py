import sqlite3
import os
import json
from pprint import pprint
from src.construction.old_sharepoint import VistasQueries


script_dir = os.path.dirname(__file__)
logs_dir = os.path.join(script_dir, "..", "logs")
json_path = os.path.join(logs_dir, "attachment_log.json")

conn = sqlite3.connect(os.path.join(logs_dir, "example.db"))
cursor = conn.cursor()
with open(json_path, "r", encoding="utf-8") as file:
    dict_db = json.load(file)


for details in dict_db:
    condition = details.get("sharepoint_uploaded", False)
    details["sharepoint_uploaded"] = 1 if condition else 0
        
#pprint(dict_db)

cursor.execute(VistasQueries().create_table)

# for details in dict_db:
#     cursor.execute(insert_query, (
#         details["author"], 
#         details["new_name"], 
#         details["original_name"], 
#         details["path"], 
#         details["sharepoint_uploaded"]
#     ))

# conn.commit()
# conn.close()