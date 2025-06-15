import pandas as pd
import firebase_admin
import re
import html
from firebase_admin import credentials, firestore
import ast
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


# === Firestore Setup ===
if not firebase_admin._apps:
    cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Prod_Scripts\slidez_important.json')  # 🔁 Update this path
    firebase_admin.initialize_app(cred)
db = firestore.client()

# === Logging Setup ===
logging.basicConfig(
    filename="update_categories.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Category Parser ===
def parse_categories(value):
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list) and parsed:
            return parsed[0].strip()
    except Exception as e:
        logging.error(f"Category parse error: {value} | {e}")
    return value.strip() if isinstance(value, str) else value


def clean_description(value):
    if not isinstance(value, str):
        return value
    value = value.strip()
    # Decode HTML entities like &nbsp;, &quot;
    value = html.unescape(value)
    # Remove surrounding double quotes if present
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1].strip()
    # Remove smart quotes and remaining double quotes, preserve apostrophes
    value = value.replace('“', '').replace('”', '').replace('"', '')
    # Remove excess punctuation (e.g., !!!, ???, ...)
    value = re.sub(r'([!?.,]){2,}', r'\1', value)
    # Remove extra whitespace
    value = re.sub(r'\s+', ' ', value)

    return value.strip()


# === Firestore Update Task ===
def update_fields_task(record, fields_to_update):
    doc_id = record.get("id")
    if not doc_id:
        return {"id": None, "error": "Missing document ID", "status": "failed"}

    update_data = {}
    for field in fields_to_update:
        raw_value = record.get(field)
        if field == "categories":
            update_data[field] = parse_categories(raw_value)
        elif field == "description":
            update_data[field] = clean_description(raw_value)
        else:
            update_data[field] = raw_value

    try:
        db.collection("products").document(doc_id).update(update_data)
        return {"id": doc_id, "status": "success"}
    except Exception as e:
        error_msg = str(e)
        logging.error(f"❌ Firestore update failed for {doc_id}: {error_msg}")
        return {"id": doc_id, **record, "error": error_msg, "status": "failed"}


# === Main Function ===
def main():
    file_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\products_output.xlsx"
    max_workers = 8
    fields_to_update = ["categories","description"]  # You can later add more fields here

    df = pd.read_excel(file_path)
    update_records = df[["id"] + fields_to_update].dropna(subset=["id"]).to_dict(orient="records")

    success_count = 0
    failed_records = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(update_fields_task, record, fields_to_update): record
            for record in update_records
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔄 Updating Firestore Fields"):
            result = future.result()
            if result["status"] == "success":
                success_count += 1
            else:
                failed_records.append(result)

    if failed_records:
        fail_df = pd.DataFrame(failed_records)
        fail_file = file_path.replace(".xlsx", "_fields_update_failed.xlsx")
        fail_df.to_excel(fail_file, index=False)
        print(f"⚠️ {len(failed_records)} failed updates saved to '{fail_file}'")

    print("\n=== Firestore Update Summary ===")
    print(f"Total:       {len(update_records)}")
    print(f"Successful:  {success_count}")
    print(f"Failed:      {len(failed_records)}")


# === Entry Point ===
if __name__ == "__main__":
    main()
