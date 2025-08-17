# inspect_firestore.py
from firebase import db

print("\n📂 Listing cities and their structure...\n")
for city_doc in db.collection("City").stream():
    print(f"City doc: {city_doc.id}")
    collections = list(city_doc.reference.collections())
    for coll in collections:
        print(f"  Subcollection: {coll.id}")
        # show one document in each subcollection for inspection
        docs = list(coll.limit(1).stream())
        for d in docs:
            print(f"    Example doc: {d.id}")
            subcols = list(d.reference.collections())
            for sub in subcols:
                print(f"      Subcollection: {sub.id}")
print("\n✅ Done.\n")
