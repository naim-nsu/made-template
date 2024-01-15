import os

BASE_DIR = os.getcwd()
SOURCE_INFO_PATH = os.path.join(BASE_DIR, "project", "config", "source_info.json")
DB_PATH = os.path.join(BASE_DIR, "project", "data", "fau_made_project_ws23.sqlite")
GENESIS_USERNAME = os.environ.get("GENESIS_USERNAME") or ''        #replace '' with your genesis username
GENESIS_PASSWORD = os.environ.get("GENESIS_PASSWORD") or ''        #replace '' with your genesis password