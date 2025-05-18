import pandas as pd
import concurrent.futures
import gc
import os
import traceback
from datetime import datetime
from tqdm import tqdm

import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firestore
if not firebase_admin._apps:
    cred = credentials.Certificate(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Prod_Scripts\slidez_important.json")  # Update path
    firebase_admin.initialize_app(cred)
db = firestore.client()

# Configuration
INPUT_EXCEL_FILE = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\slidez_final_supplierId.xlsx"  # Update path
FAILED_OUTPUT_FILE = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Raw_Data_Input\Failed\failed_products_supplierIds.xlsx"  # Update path
COLLECTION_NAME = "products"
REFERENCE_COLLECTION = "suppliers"
EXCEL_ID_COLUMN = "id"  # Product document ID
EXCEL_REFERENCE_COLUMN = "supplier_id"  # Column in Excel to use for referencing
FIRESTORE_REFERENCE_FIELD = "supplierRef"  # Field to be added in Firestore

BATCH_SIZE = 500
MAX_WORKERS = 4  # Parallel threads

# Global tracking
partial_errors = []
success_count = 0
fail_count = 0


def prepare_record_for_update(record):
    try:
        doc_id = str(record[EXCEL_ID_COLUMN]).strip()
        ref_id = str(record[EXCEL_REFERENCE_COLUMN]).strip()
        ref = db.collection(REFERENCE_COLLECTION).document(ref_id)

        update_data = {
            EXCEL_REFERENCE_COLUMN: ref_id,
            FIRESTORE_REFERENCE_FIELD: ref
        }
        return doc_id, update_data, None
    except Exception as e:
        return None, None, {
            "id": record.get(EXCEL_ID_COLUMN),
            "field": FIRESTORE_REFERENCE_FIELD,
            "raw": record,
            "error": str(e)
        }


def upload_batch(batch_records):
    global success_count, fail_count
    batch = db.batch()
    local_success = 0
    for doc_id, update_data in batch_records:
        try:
            doc_ref = db.collection(COLLECTION_NAME).document(doc_id)
            batch.update(doc_ref, update_data)
            local_success += 1
        except Exception as e:
            partial_errors.append({
                "id": doc_id,
                "field": "batch_update",
                "raw": update_data,
                "error": str(e)
            })

    try:
        batch.commit()
        success_count += local_success
    except Exception as e:
        fail_count += len(batch_records)
        for doc_id, update_data in batch_records:
            partial_errors.append({
                "id": doc_id,
                "field": "batch_commit",
                "raw": update_data,
                "error": str(e)
            })


def process_and_upload(records):
    processed_records = []
    for record in records:
        doc_id, update_data, err = prepare_record_for_update(record)
        if doc_id and update_data:
            processed_records.append((doc_id, update_data))
        if err:
            partial_errors.append(err)

    batches = [processed_records[i:i + BATCH_SIZE] for i in range(0, len(processed_records), BATCH_SIZE)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(upload_batch, batches), total=len(batches), desc="Updating Firestore"))
    gc.collect()


def write_partial_errors_to_excel():
    if partial_errors:
        for err in partial_errors:
            if isinstance(err.get("raw"), (dict, list)):
                err["raw"] = str(err["raw"])
        df_errors = pd.DataFrame(partial_errors)
        df_errors.to_excel(FAILED_OUTPUT_FILE, index=False)


def main():
    print("Reading Excel file...")
    try:
        df = pd.read_excel(INPUT_EXCEL_FILE)
        if EXCEL_ID_COLUMN not in df.columns or EXCEL_REFERENCE_COLUMN not in df.columns:
            raise ValueError(f"Missing required columns: {EXCEL_ID_COLUMN}, {EXCEL_REFERENCE_COLUMN}")
        records = df.to_dict(orient='records')
    except Exception as e:
        print("Failed to read Excel:", e)
        return

    process_and_upload(records)
    write_partial_errors_to_excel()

    print("\n=== Summary ===")
    print(f"Successful updates: {success_count}")
    print(f"Failed updates: {fail_count}")
    print(f"Partial errors: {len(partial_errors)}")


if __name__ == "__main__":
    main()
