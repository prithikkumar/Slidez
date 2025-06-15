import pandas as pd
import concurrent.futures
import gc
import os
import re
import ast
import traceback
from datetime import datetime
from tqdm import tqdm

import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firestore
if not firebase_admin._apps:
    cred = credentials.Certificate(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\slidez_important.json")
    firebase_admin.initialize_app(cred)
db = firestore.client()

# Configuration
INPUT_EXCEL_FILE = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\output_file.xlsx"
FAILED_OUTPUT_FILE = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\retry_failed_records.xlsx"
COLLECTION_NAME = "products"
BATCH_SIZE = 500
MAX_WORKERS = 4  # Parallel threads for batch uploading

# Global tracking
partial_errors = []
success_count = 0
fail_count = 0


def parse_available_options(raw, item_id):
    try:
        if pd.isna(raw) or str(raw).strip() == '':
            return []
        val = re.sub(r"array\((\[.*?\])(?:,\s*dtype=[^)]+)?\)", r"\1", str(raw))
        parsed = ast.literal_eval(val)
        for option in parsed:
            if isinstance(option.get('values'), list):
                option['values'] = [str(v) for v in option['values']]
        return parsed
    except Exception as e:
        partial_errors.append({
            "id": item_id,
            "field": "availableOptions",
            "raw": raw,
            "error": str(e)
        })
        return []


def prepare_record(record):
    item_id = record.get("id")
    try:
        record["availableOptions"] = parse_available_options(record.get("availableOptions"), item_id)
        return record, None
    except Exception as e:
        return None, {"id": item_id, "field": "general", "raw": record, "error": str(e)}


def upload_batch(batch_records):
    global success_count, fail_count
    batch = db.batch()
    local_success = 0
    for record in batch_records:
        try:
            doc_ref = db.collection(COLLECTION_NAME).document(str(record["id"]))
            batch.set(doc_ref, record, merge=True)
            local_success += 1
        except Exception as e:
            partial_errors.append({
                "id": record.get("id"),
                "field": "firestore",
                "raw": record,
                "error": str(e)
            })
    try:
        batch.commit()
        success_count += local_success
    except Exception as e:
        fail_count += len(batch_records)
        for record in batch_records:
            partial_errors.append({
                "id": record.get("id"),
                "field": "commit",
                "raw": record,
                "error": str(e)
            })


def process_and_upload(df_chunk):
    processed_records = []
    for record in df_chunk:
        rec, err = prepare_record(record)
        if rec:
            processed_records.append(rec)
        if err:
            partial_errors.append(err)

    batches = [processed_records[i:i + BATCH_SIZE] for i in range(0, len(processed_records), BATCH_SIZE)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(upload_batch, batches), total=len(batches), desc="Uploading"))
    gc.collect()


def write_partial_errors_to_excel():
    if partial_errors:
        # Ensure raw is stringified for Excel safety
        for err in partial_errors:
            if isinstance(err.get("raw"), (dict, list)):
                err["raw"] = str(err["raw"])
        df_errors = pd.DataFrame(partial_errors)
        df_errors.to_excel(FAILED_OUTPUT_FILE, index=False)


def main():
    print("Reading data from Excel...")
    try:
        df = pd.read_excel(INPUT_EXCEL_FILE)
        all_records = df.to_dict(orient='records')
    except Exception as e:
        print("Failed to read Excel file:", e)
        return

    process_and_upload(all_records)
    write_partial_errors_to_excel()

    print("\n=== Summary ===")
    print(f"Successful uploads: {success_count}")
    print(f"Failed uploads: {fail_count}")
    print(f"Partial errors captured: {len(partial_errors)}")


if __name__ == "__main__":
    main()
