from pymongo import MongoClient
from config.constants import MONGO_URI

client = MongoClient(MONGO_URI)
db = client["orchestrator"]

task_collection = db["tasks"]
workers_collection = db["workers"]