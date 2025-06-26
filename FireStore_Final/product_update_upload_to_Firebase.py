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
    filename="update_all_fields.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Firestore Update Task ===
def update_full_document(record):
    doc_id = record.get("id")
    if not doc_id:
        return {"id": None, "error": "Missing document ID", "status": "failed"}

    try:
        # 🔁 set(..., merge=False) will replace the entire document
        db.collection("products").document(doc_id).set(record, merge=False)
        return {"id": doc_id, "status": "success"}
    except Exception as e:
        error_msg = str(e)
        logging.error(f"❌ Firestore update failed for {doc_id}: {error_msg}")
        return {"id": doc_id, "error": error_msg, "status": "failed"}

# === Main Function ===
def main():
    file_path = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\products_final.jsonl'  # 🔁 Update if needed
    max_workers = 8

    update_records = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                if "id" in record:
                    update_records.append(record)
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON: {e}")

    success_count = 0
    failed_records = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(update_full_document, record): record
            for record in update_records
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔄 Updating Firestore Documents"):
            result = future.result()
            if result["status"] == "success":
                success_count += 1
            else:
                failed_records.append(result)

    # === Save Failed Records ===
    if failed_records:
        fail_file = file_path.replace(".jsonl", "_failed_updates.jsonl")
        with open(fail_file, "w", encoding="utf-8") as f:
            for rec in failed_records:
                f.write(json.dumps(rec) + "\n")
        print(f"⚠️ {len(failed_records)} failed updates saved to '{fail_file}'")

    # === Summary ===
    print("\n=== Firestore Update Summary ===")
    print(f"Total:      {len(update_records)}")
    print(f"Successful: {success_count}")
    print(f"Failed:     {len(failed_records)}")

if __name__ == "__main__":
    main()
