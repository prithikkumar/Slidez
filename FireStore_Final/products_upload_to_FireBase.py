import json
import logging
import firebase_admin
from firebase_admin import credentials, firestore
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# === Firestore Setup ===
if not firebase_admin._apps:
    cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\FireStore_Final\slidez_important.json')  # 🔁 Update this path
    firebase_admin.initialize_app(cred)
db = firestore.client()

# === Logging Setup ===
logging.basicConfig(
    filename="insert_only.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Firestore Insert Task ===
def insert_new_document(record):
    doc_id = record.get("id")
    if not doc_id:
        return {"id": None, "error": "Missing document ID", "status": "failed"}
    
    try:
        # 🔒 .create() fails if the document already exists
        db.collection("products").document(doc_id).create(record)
        return {"id": doc_id, "status": "success"}
    except Exception as e:
        error_msg = str(e)
        logging.error(f"❌ Firestore insert failed for {doc_id}: {error_msg}")
        return {"id": doc_id, "error": error_msg, "status": "failed"}

# === Main Function ===
def main():
    file_path = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\products_final_28_06.jsonl'  # 🔁 Update if needed
    max_workers = 8

    insert_records = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                if "id" in record:
                    insert_records.append(record)
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON: {e}")

    success_count = 0
    failed_records = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(insert_new_document, record): record
            for record in insert_records
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="🆕 Inserting New Firestore Documents"):
            result = future.result()
            if result["status"] == "success":
                success_count += 1
            else:
                failed_records.append(result)

    # === Save Failed Inserts ===
    if failed_records:
        fail_file = file_path.replace(".jsonl", "_insert_failed.jsonl")
        with open(fail_file, "w", encoding="utf-8") as f:
            for rec in failed_records:
                f.write(json.dumps(rec) + "\n")
        print(f"⚠️ {len(failed_records)} failed inserts saved to '{fail_file}'")

    # === Summary ===
    print("\n=== Firestore Insert Summary ===")
    print(f"Total:      {len(insert_records)}")
    print(f"Inserted:   {success_count}")
    print(f"Failed:     {len(failed_records)}")

if __name__ == "__main__":
    main()
