# wipe_all_dates.py
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent))  # local import

from firebase import db
import argparse

BATCH = 450  # Firestore limit safety

def delete_subcollection(coll_ref, dry_run: bool) -> int:
    """Delete all docs in a subcollection in batches; return count."""
    deleted = 0
    while True:
        docs = list(coll_ref.limit(BATCH).stream())
        if not docs:
            break
        if dry_run:
            deleted += len(docs)
            # only count one page on dry run (avoid heavy reads)
            break
        batch = db.batch()
        for d in docs:
            batch.delete(d.reference)
        batch.commit()
        deleted += len(docs)
    return deleted

def wipe_all_dates(dry_run: bool = False):
    total_cities = 0
    total_hotels = 0
    total_dates = 0
    total_rooms = 0

    print(f"{'🔎 DRY RUN' if dry_run else '🧹 Deleting'} all Dates and Rooms under every Hotel in 'City' …")

    for city_doc in db.collection("City").stream():
        total_cities += 1
        city_ref = city_doc.reference

        for hotel_doc in city_ref.collection("Hotels").stream():
            total_hotels += 1
            hotel_ref = hotel_doc.reference

            for date_doc in hotel_ref.collection("Dates").stream():
                total_dates += 1
                rooms_ref = date_doc.reference.collection("Rooms")
                total_rooms += delete_subcollection(rooms_ref, dry_run=dry_run)
                if not dry_run:
                    date_doc.reference.delete()

    print(
        f"✅ {'Would remove' if dry_run else 'Removed'} "
        f"{total_rooms} room docs across {total_dates} date docs "
        f"from {total_hotels} hotels in {total_cities} cities."
    )

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Delete ALL Dates (and Rooms) under every Hotel, keeping City/Hotel docs.")
    ap.add_argument("--dry-run", action="store_true", help="Preview counts without deleting")
    args = ap.parse_args()
    wipe_all_dates(dry_run=args.dry_run)
