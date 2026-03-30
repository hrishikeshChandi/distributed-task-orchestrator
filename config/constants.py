from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.getenv("HOST", "127.0.0.1")
MODULE = os.getenv("MODULE", "main:app")
PORT = int(os.getenv("PORT", "8000"))
MONGO_URI = os.getenv("MONGO_URI")

TIMEOUT = 30
MAX_RETRIES = 3

# ✅ NEW
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 30
