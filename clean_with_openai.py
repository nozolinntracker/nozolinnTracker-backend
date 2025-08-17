# clean_with_openai.py

import os
import json
import re
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI

# ============== Load environment & OpenAI client ==============
load_dotenv()
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ============== Allowed room names ==============
allowed_rooms = {
    "emaar legend": {
        "standard twin room - ro",
        "standard double room - ro",
        "standard triple room - ro",
        "standard quad room - ro"
    },
    "jabal omar hyatt regency": {
        "standard quad room - bb",
        "standard triple room - bb",
        "standard twin room (haram view) - bb",
        "standard double room (haram view) - bb",
        "standard double room (haram view) - ro",
        "standard twin room (haram view) - ro",
        "standard quad room - ro",
        "standard triple room - ro",
        "standard double room - bb",
        "standard double room - ro",
        "standard twin room ro"
    },
    "al ebaa hotel": {
        "standard twin room - ro",
        "standard double room - ro",
        "standard triple room - ro",
        "standard quad room - ro",
        "standard twin room - bb",
        "standard double room - bb",
        "standard triple room - bb",
        "standard quad room - bb"
    },
    "elaf ajyad": {
        "standard room - ro",
        "standard double room - ro",
        "standard triple room - ro",
        "standard quad room - ro",
        "standard room - bb",
        "standard double room - bb",
        "standard triple room - bb",
        "standard quad room - bb"
    },
    "makarem ajyad makkah hotel": {
        "standard twin room - ro",
        "standard double room - ro",
        "standard triple room - ro",
        "standard quad room - ro",
        "standard twin room - bb",
        "standard double room - bb",
        "standard triple room - bb",
        "standard quad room - bb"
    },
    "Zaha Al Munawara Hotel": {
        "standard Twin Room (2 Single Beds) – bb",
        "standard Triple Room – bb",
        "standard Quad Room – bb"
    },
    "Hafawah Suites": {
        "Executive Suite (2 Adults) - ro",
        "Executive Suite (3 Adults) - ro",
        "Executive Suite (4 Adults) - ro"
    },
    "New Madinah Hotel": {
        "standard Double Room - ro",
        "standard Double King – ro",
        "standard Triple Room – ro",
        "standard Quad Room – ro",
        "standard Twin Room (2 Single Beds) – bb",
        "standard Double Room (1 Double Bed) – bb",
        "standard Triple Room – bb",
        "standard Quad Room – bb"
    },
    "Crowne Plaza Madinah": {
        "standard Double Room - ro",
        "standard Double King – ro",
        "standard Triple Room – ro",
        "standard Twin Room (2 Single Beds) – bb",
        "standard Double Room (1 Double Bed) – bb",
        "standard Triple Room – bb"
    },
    "Saja Al Madinah": {
        "standard Twin Room - ro",
        "standard Double Room – ro",
        "standard Triple Room – ro",
        "standard Quad Room – ro",
        "standard Twin Room – bb",
        "standard Double Room – bb",
        "standard Triple Room – bb",
        "standard Quad Room – bb"
    }
}

# ============== Helpers ==============
def normalize(text: str) -> str:
    return (text or "").strip().lower()

def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

# --- Rule lists ---
ROOM_SKIP_TOKENS = ["club", "royal", "standard room", "bo","family","classic","economy","executive"]  # discard if in RAW room name
MEAL_REJECT_TOKENS = ["lunch", "dinner", "buffet", "hb", "fb", "half board", "full board","coffee","tea"]

MEAL_RO_TOKENS = [
    "no breakfast", "without breakfast", "breakfast not included", "room only", "ro",
    "excluding breakfast", "without meals", "room without breakfast", "no meal included", "meals not included", "none"
]

MEAL_BB_TOKENS = [
    "bed & breakfast", "bed and breakfast", "with breakfast", "breakfast included", "bb",
    "breakfast board", "free breakfast", "breakfast, free wifi", "full breakfast","breakfast,free wifi"
]

# ============== Cache ==============
cache_file = "classification_cache.json"
if os.path.exists(cache_file):
    with open(cache_file, "r", encoding="utf-8") as f:
        classification_cache = json.load(f)
else:
    classification_cache = {}

def save_cache():
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(classification_cache, f, ensure_ascii=False, indent=2)

# ============== GPT classifier ==============
async def classify_room(hotel, room_name, meal_plan, allowed_set):
    """
    Only called for normalized meals 'ro' or 'bb'.
    Returns one of allowed_set (lowercased) or 'ignore'.
    """
    key = f"{hotel}|||{room_name}|||{meal_plan}"
    if key in classification_cache:
        print(f"🧠 Using cached result → {classification_cache[key]}")
        return classification_cache[key]

    prompt = f"""
Hotel: {hotel}
Room: {room_name}
Meal plan: {meal_plan}

Your task:
1. Match the room to one of the allowed room names.
2. Accept only 'ro' or 'bb' meals (room only or breakfast).
3. Reject if meal contains lunch, dinner, buffet, fb or hb (or half/full board).
4. Do not explain your choice.
5. Return only the room name from the allowed list (e.g. 'standard twin room - bb').
6. If no match is found, return exactly: ignore
7. If two names have the same meaning, pick the closest allowed name.

Allowed room names:
{chr(10).join(f"- {opt}" for opt in allowed_set)}
""".strip()

    try:
        print(f"🔎 GPT: Room='{room_name}', Meal='{meal_plan}'")
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a strict hotel room classifier. Match to the most similar allowed room. Only return the exact allowed room name or 'ignore'."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=50
        )
        raw = (response.choices[0].message.content or "").strip().lower()
        cleaned = None

        # Exact or contains match against allowed_set
        for option in allowed_set:
            opt_low = option.lower()
            if opt_low == raw or opt_low in raw:
                cleaned = opt_low
                break

        if not cleaned and "ignore" in raw:
            cleaned = "ignore"
        if not cleaned:
            print(f"⚠️ Unexpected GPT output → {raw}")
            cleaned = "ignore"

        print(f"✅ GPT classified as → {cleaned}")
        classification_cache[key] = cleaned
        return cleaned
    except Exception as e:
        print("❌ OpenAI API error:", e)
        return "ignore"

# ============== Main cleaner ==============
async def clean_with_gpt():
    input_folder = "hotel_data"
    output_folder = "cleaned_data"
    os.makedirs(output_folder, exist_ok=True)

    for filename in sorted(os.listdir(input_folder)):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(input_folder, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)
        if not records:
            continue

        # Identify hotel by fuzzy key containment
        hotel_raw = normalize(records[0].get("H", ""))
        matched_key = None
        for known in allowed_rooms:
            if normalize(known) in hotel_raw:
                matched_key = known
                break

        if not matched_key:
            print(f"⏭️ Skipping: '{hotel_raw}' — no match in allowed list")
            continue

        allowed_set = allowed_rooms[matched_key]
        allowed_lower = {x.lower() for x in allowed_set}

        cleaned = []
        accepted_room_types = set()  # final accepted in this file
        # candidates we may use later if twin/double missing
        # structure: list of (original_record, normalized_meal, candidate_type)
        # candidate_type in {"twin_or_double", "king", "queen"}
        candidates = []

        print(f"\n🔍 Cleaning: {filename} ({len(records)} records) — Hotel key: {matched_key}")

        for record in records:
            raw_room = normalize(record.get("R", ""))
            raw_meal = normalize(record.get("M", ""))

            if not raw_room or raw_room in ["n/a"]:
                print(f"⏭️ Skipping empty or N/A room → {record}")
                continue

            if not matched_key:
                print(f"⏭️ Skipping: '{hotel_raw}' — no match in allowed list")
                continue

            allowed_set = allowed_rooms[matched_key]
            allowed_lower = {x.lower() for x in allowed_set}

            # ✅ Add this line so hotel_key_norm exists
            hotel_key_norm = normalize(matched_key)

            # ---------- Discard by RAW room tokens ----------
            if hotel_key_norm != "hafawah suites" and any(tok in raw_room for tok in ROOM_SKIP_TOKENS):
                print(f"⏭️ Discard by room token ({ROOM_SKIP_TOKENS}) → {raw_room}")
                continue

            # ---------- Detect candidates (do not classify now) ----------
            # twin or double (with slash or the word 'or')
            if re.search(r"\b(twin|twn)\s*(/|or)\s*(double|dbl)\b", raw_room) or \
               re.search(r"\b(double|dbl)\s*(/|or)\s*(twin|twn)\b", raw_room):
                candidates.append((record, None, "twin_or_double"))
                print(f"🗂️ Stashed candidate (twin_or_double) → {raw_room}")
                continue

            if re.search(r"\bking\b", raw_room):
                candidates.append((record, None, "king"))
                print(f"🗂️ Stashed candidate (king) → {raw_room}")
                continue

            if re.search(r"\bqueen\b", raw_room):
                candidates.append((record, None, "queen"))
                print(f"🗂️ Stashed candidate (queen) → {raw_room}")
                continue

            # ---------- Meal rejection ----------
            if any(tok in raw_meal for tok in MEAL_REJECT_TOKENS):
                print(f"⏭️ Discard by meal token ({MEAL_REJECT_TOKENS}) → {raw_meal}")
                continue

            # Remove noise like 'free wifi' but keep meaning
            meal_clean = norm_spaces(raw_meal.replace("free wifi", ""))

            # ---------- Meal normalization ----------
            normalized_meal = None
            if any(tok in meal_clean for tok in MEAL_RO_TOKENS):
                normalized_meal = "ro"
            elif any(tok in meal_clean for tok in MEAL_BB_TOKENS):
                normalized_meal = "bb"
            else:
                # Unknown → FLAG and do not classify
                record["flagged_meal"] = raw_meal
                record["normalized_meal"] = f"FLAG:{raw_meal}"
                print(f"🚩 Flagging meal (unrecognized) → {raw_meal}")
                # Keep the raw record in output for visibility (optional: comment next two lines to exclude)
                # cleaned.append(record)
                # continue
                # Safer per your instruction: keep but DO NOT classify
                cleaned.append(record)
                continue

            # ---------- Classify with GPT (only RO/BB) ----------
            classification = await classify_room(hotel_raw, raw_room, normalized_meal, allowed_set)
            if classification != "ignore":
                record["normalized_room_type"] = classification
                record["normalized_meal"] = normalized_meal
                cleaned.append(record)
                accepted_room_types.add(classification)

        # ---------- Post-pass: fill missing twin/double using candidates ----------
        # For both meals that appear in allowed_set, if missing, try to promote a candidate
        def need_and_allowed(kind: str, meal: str) -> bool:
            key = f"standard {kind} room - {meal}".lower()
            return key in allowed_lower and key not in accepted_room_types

        # For each meal type we care about:
        for meal in ("ro", "bb"):
            # twin missing?
            if need_and_allowed("twin", meal):
                # look for a candidate with same meal:
                # we don't know candidate meal yet; compute it now per the rules
                picked = None
                for idx, (rec, _nm, ctype) in enumerate(candidates):
                    raw_meal = normalize(rec.get("M", ""))
                    if any(tok in raw_meal for tok in MEAL_REJECT_TOKENS):
                        continue
                    meal_clean = norm_spaces(raw_meal.replace("free wifi", ""))

                    if any(tok in meal_clean for tok in MEAL_RO_TOKENS):
                        nm = "ro"
                    elif any(tok in meal_clean for tok in MEAL_BB_TOKENS):
                        nm = "bb"
                    else:
                        continue  # cannot use flagged meals to fill

                    if nm != meal:
                        continue

                    if ctype == "twin_or_double":
                        picked = (idx, rec)
                        break
                if picked:
                    _, rec = picked
                    synth = dict(rec)
                    synth["normalized_meal"] = meal
                    synth["normalized_room_type"] = f"standard twin room - {meal}"
                    cleaned.append(synth)
                    accepted_room_types.add(synth["normalized_room_type"])
                    print(f"➕ Filled missing TWIN ({meal}) from candidate.")
            # double missing?
            if need_and_allowed("double", meal):
                picked = None
                # prefer twin_or_double; else king/queen → double
                for preference in ("twin_or_double", "king", "queen"):
                    for idx, (rec, _nm, ctype) in enumerate(candidates):
                        if ctype != preference:
                            continue
                        raw_meal = normalize(rec.get("M", ""))
                        if any(tok in raw_meal for tok in MEAL_REJECT_TOKENS):
                            continue
                        meal_clean = norm_spaces(raw_meal.replace("free wifi", ""))

                        if any(tok in meal_clean for tok in MEAL_RO_TOKENS):
                            nm = "ro"
                        elif any(tok in meal_clean for tok in MEAL_BB_TOKENS):
                            nm = "bb"
                        else:
                            continue
                        if nm != meal:
                            continue
                        picked = (idx, rec)
                        break
                    if picked:
                        break
                if picked:
                    _, rec = picked
                    synth = dict(rec)
                    synth["normalized_meal"] = meal
                    synth["normalized_room_type"] = f"standard double room - {meal}"
                    cleaned.append(synth)
                    accepted_room_types.add(synth["normalized_room_type"])
                    print(f"➕ Filled missing DOUBLE ({meal}) from candidate ({preference}).")

        # ====== Save cleaned file ======
        if cleaned:
            output_path = os.path.join(output_folder, filename)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(cleaned, f, ensure_ascii=False, indent=2)
            print(f"✅ Saved cleaned file → {output_path} ({len(cleaned)} entries)")
        else:
            print(f"⚠️ No matching rooms found in {filename}")

    save_cache()

# ============== Entrypoint ==============
if __name__ == "__main__":
    try:
        asyncio.run(clean_with_gpt())
    finally:
        save_cache()
