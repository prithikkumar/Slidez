import pandas as pd
import firebase_admin
import os
from firebase_admin import credentials, firestore
from tqdm import tqdm

# === Firebase Init ===
cred = credentials.Certificate(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Prod_Scripts\slidez_important.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# === Config ===
INPUT_EXCEL = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\suppliers_output.xlsx"
UNMAPPED_OUTPUT = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Raw_Data\Failed\supplier_unmapped_fulfill_times.xlsx"
COLLECTION_NAME = "suppliers"
BATCH_SIZE = 500

# === Fulfill Time Mapping ===
FULFILL_TIME_MAPPING = {
    "LESS_THAN_TWENTY_FOUR_HOURS": "Less than 24 hours",
    "TWENTY_FOUR_TO_FORTY_EIGHT_HOURS": "24-48 hours",
    "FORTY_EIGHT_TO_SEVENTY_TWO_HOURS": "48-72 hours",
    "MORE_THAN_SEVENTY_TWO_HOURS": "More than 72 hours",
    "ONE_TO_TWO_WEEKS": "1-2 weeks",
    "TWO_TO_FOUR_WEEKS": "2-4 weeks",
    "MORE_THAN_FOUR_WEEKS": "More than 4 weeks",
    "FIVE_TO_SEVEN_DAYS": "5-7 days",
    "TWO_TO_FOUR_DAYS": "2-4 days",
    "FOUR_WEEKS_PLUS": "4+ weeks"
}

unmapped_records = []

def normalize_fulfill_time(value):
    if pd.isna(value):  # Properly handle actual NaN (missing) values
        return None
    normalized = str(value).strip().upper()
    return FULFILL_TIME_MAPPING.get(normalized, None)

def update_supplier_avg_fulfill_time(records):
    updated = 0
    for i in tqdm(range(0, len(records), BATCH_SIZE), desc="Updating fulfill times"):
        batch = db.batch()
        chunk = records[i:i + BATCH_SIZE]
        for record in chunk:
            supplier_id = str(record.get("supplier_id", "")).strip()
            raw_value = record.get("supplier_avg_fulfill_time", None)
            if not supplier_id:
                continue

            mapped_value = normalize_fulfill_time(raw_value)
            if mapped_value:
                doc_ref = db.collection(COLLECTION_NAME).document(supplier_id)
                batch.update(doc_ref, {"supplier_avg_fulfill_time": mapped_value})
                updated += 1
            else:
                unmapped_records.append({
                    "supplier_id": supplier_id,
                    "raw_value": raw_value
                })

        batch.commit()

    return updated

# === Main ===
def main():
    df = pd.read_excel(INPUT_EXCEL, dtype=str)
    records = df.to_dict(orient="records")

    updated_count = update_supplier_avg_fulfill_time(records)

    if unmapped_records:
        os.makedirs(os.path.dirname(UNMAPPED_OUTPUT), exist_ok=True)
        pd.DataFrame(unmapped_records).drop_duplicates().to_excel(UNMAPPED_OUTPUT, index=False)
        print(f"\n⚠️ Unmapped values saved to: {UNMAPPED_OUTPUT}")

    print(f"\n✅ Updated {updated_count} supplier_avg_fulfill_time records.")

if __name__ == "__main__":
    main()
