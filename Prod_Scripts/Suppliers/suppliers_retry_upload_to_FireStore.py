import pandas as pd
import re
from tqdm import tqdm
import firebase_admin
from firebase_admin import credentials, firestore

# === Firebase Init ===
cred = credentials.Certificate(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\FireStore_Push\slidez_important.json")
firebase_admin.initialize_app(cred, name='retryApp')
db = firestore.client(firebase_admin.get_app('retryApp'))

# === Configuration ===
FAILED_INPUT_PATH = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Raw_Data\Failed\supplier_failed_records.xlsx"
FAILED_OUTPUT_PATH = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Raw_Data\Failed\supplier_failed_retry_records.xlsx"  # Overwrite same file
COLLECTION_NAME = "suppliers"
BATCH_SIZE = 500

# === Helper Functions ===

def parse_boolean(value):
    true_vals = {'true', '1', 'yes', 'y', '✓'}
    false_vals = {'false', '0', 'no', 'n', '✗'}
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        raise ValueError("Boolean field is empty or NaN")
    val_str = str(value).strip().lower()
    if val_str in true_vals:
        return True
    elif val_str in false_vals:
        return False
    else:
        raise ValueError(f"Invalid boolean value: {value}")

def parse_phone_number(value):
    phone_str = str(value)
    digits_only = re.sub(r"\D", "", phone_str)
    if not digits_only:
        raise ValueError("No valid digits found in phone number")
    return int(digits_only)  # <- store as integer


def safe_float(value):
    try:
        return float(value)
    except:
        return None  # Skip invalid float values

# === Retry Validator ===

def validate_and_parse_retry(record):
    parsed = {}
    errors = []

    for field, value in record.items():
        value = str(value).strip() if pd.notna(value) else ""

        if value == "":
            parsed[field] = value
            continue

        try:
            if field == "request_date":
                parsed[field] = pd.to_datetime(value)
            elif "phone" in field:
                parsed[field] = parse_phone_number(value)
            elif field in ["share_customer_data", "accepts_returns", "shipping_us_fee_enabled",
                           "shipping_us_free_threshold_enabled", "shipping_intl_fee_enabled",
                           "shipping_intl_free_threshold_enabled", "partner_request_is_deleted"]:
                parsed[field] = parse_boolean(value)
            elif field in ["supplier_cut", "retailer_cut", "carro_cut", "default_supplier_cut",
                           "shipping_us_price", "shipping_us_free_threshold", "shipping_intl_price",
                           "shipping_intl_free_threshold"]:
                fval = safe_float(value)
                if fval is None:
                    raise ValueError(f"Cannot convert to float: {value}")
                parsed[field] = fval
            else:
                parsed[field] = value
        except Exception as e:
            errors.append({
                "supplier_id": record.get("supplier_id", ""),
                "field_name": field,
                "raw_data": value,
                "error": str(e)
            })

    return parsed, errors

# === Firestore Retry Upload ===
def retry_upload(records):
    success_count = 0
    failed_records = []

    for i in tqdm(range(0, len(records), BATCH_SIZE), desc="Retry Uploading"):
        batch = db.batch()
        batch_records = records[i:i+BATCH_SIZE]

        for record in batch_records:
            parsed, errors = validate_and_parse_retry(record)
            if errors:
                record['validation_errors'] = str(errors)
                failed_records.append(record)
                continue
            try:
                doc_ref = db.collection(COLLECTION_NAME).document(parsed["supplier_id"])
                batch.set(doc_ref, parsed)
                success_count += 1
            except Exception as e:
                record['upload_error'] = str(e)
                failed_records.append(record)

        try:
            batch.commit()
        except Exception as e:
            for r in batch_records:
                r['batch_commit_error'] = str(e)
                failed_records.append(r)
            success_count -= len(batch_records)

    return success_count, failed_records

# === Main ===
def main():
    df = pd.read_excel(FAILED_INPUT_PATH, dtype=str)
    records = df.to_dict(orient="records")
    initial_count = len(records)

    success_count, failed_records = retry_upload(records)

    if failed_records:
        pd.DataFrame(failed_records).to_excel(FAILED_OUTPUT_PATH, index=False)

    print("\n=== Retry Upload Summary ===")
    print(f"Total Failed Records Retried : {initial_count}")
    print(f"Successfully Uploaded        : {success_count}")
    print(f"Still Failed                 : {len(failed_records)}")
    print(f"Failed File                  : {FAILED_OUTPUT_PATH if failed_records else 'None'}")

if __name__ == "__main__":
    main()
