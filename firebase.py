# firebase_setup.py
from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore

BASE_DIR = Path(__file__).resolve().parent
KEY_PATH = BASE_DIR / "serviceAccountKey.json"

if not KEY_PATH.exists():
    raise FileNotFoundError(f"Firebase key not found at: {KEY_PATH}")

cred = credentials.Certificate(str(KEY_PATH))
firebase_admin.initialize_app(cred)

db = firestore.client()
