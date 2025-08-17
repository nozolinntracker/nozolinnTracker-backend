# save_nested.py
from datetime import datetime
from hashlib import sha1
from typing import List, Dict, Any
from firebase import db
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

def _slug(s: str) -> str:
    return "".join(c.lower() if c.isalnum() else "-" for c in s).strip("-")

def _as_date(value) -> datetime:
    """Accept 'YYYY-MM-DD', 'DD/MM/YYYY', 'DD-MM-YYYY', or datetime."""
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {s}")

import re

def _room_doc_id(room_name: str, meal_plan: str) -> str:
    """
    Create a readable doc ID like 'Standard Triple Room - Breakfast Not Included'.
    Firestore doc IDs must not contain '/', so we replace it. We also trim length.
    """
    meal = (meal_plan or "").strip() or "RO"
    base = f"{room_name.strip()} - {meal}"
    # make it Firestore-safe
    safe = base.replace("/", "／")           # avoid path separator
    safe = re.sub(r"\s+", " ", safe).strip()  # collapse spaces
    if len(safe) > 140:                      # keep IDs reasonable
        safe = safe[:140].rstrip()
    return safe

def save_cleaned_rows_nested(cleaned_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    cleaned_rows item example:
    {
      "city": "Makkah",
      "hotel": "Emaar Legend",
      "date": "31-08-2025",         # any accepted format; converted to Timestamp
      "room_name": "Standard Twin Room - RO",
      "meal_plan": "RO",
      "price": 350.0,
      "currency": "SAR",
      "available": True,
      "source": "myhotels.sa",
      "scraped_at": None
    }
    """
    if not cleaned_rows:
        return {"written": 0, "batches": 0}

    written = 0
    batch = db.batch()
    ops_in_batch = 0
    max_ops = 450

    for row in cleaned_rows:
        city  = row["city"].strip()
        hotel = row["hotel"].strip()
        date  = _as_date(row["date"])
        room_name = row["room_name"].strip()
        meal_plan = (row.get("meal_plan") or "").strip()

        # Path: City/<city>/Hotels/<hotel>/Dates/<yyyy-mm-dd>/Rooms/<hash>
        city_ref   = db.collection("City").document(_slug(city))
        hotel_ref  = city_ref.collection("Hotels").document(_slug(hotel))
        date_ref   = hotel_ref.collection("Dates").document(date.strftime("%Y-%m-%d"))
        room_id = _room_doc_id(room_name, meal_plan)
        room_ref = date_ref.collection("Rooms").document(room_id)

        payload = {
            "city": city,
            "hotel": hotel,
            "date": date,
            "room_name": room_name,
            "meal_plan": meal_plan,
            "price": float(row["price"]) if row.get("price") is not None else None,
            "currency": row.get("currency", "SAR"),
            "available": bool(row.get("available", True)),
            "source": row.get("source", "myhotels.sa"),
            "scraped_at": row.get("scraped_at") or SERVER_TIMESTAMP,
        }

        batch.set(room_ref, payload, merge=True)
        ops_in_batch += 1
        written += 1

        if ops_in_batch >= max_ops:
            batch.commit()
            batch = db.batch()
            ops_in_batch = 0

    if ops_in_batch:
        batch.commit()

    return {"written": written, "batches": (written // max_ops) + (1 if written % max_ops else 0)}
