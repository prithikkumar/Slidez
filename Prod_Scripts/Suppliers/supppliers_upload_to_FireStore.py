import pandas as pd
import re
from tqdm import tqdm
import firebase_admin
from firebase_admin import credentials, firestore

# === Firebase Init ===
cred = credentials.Certificate(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\FireStore_Push\slidez_important.json")  # <-- Update path
firebase_admin.initialize_app(cred)
db = firestore.client()

# === Configuration ===
EXCEL_PATH = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\suppliers_output.xlsx"
FAILED_OUTPUT_PATH = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Raw_Data\Failed\supplier_failed_records.xlsx"
COLLECTION_NAME = "suppliers"
BATCH_SIZE = 500

# === Helper Functions ===

# Boolean Validator
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

# Phone Number Validator (Fixing the format)
def parse_phone_number(value):
    phone_str = str(value)
    digits_only = re.sub(r"\D", "", phone_str)
    if not digits_only:
        raise ValueError("No valid digits found in phone number")
    return int(digits_only)  # <- store as integer


# === Field Validators ===

def validate_and_parse(record):
    parsed = {}

    for field, value in record.items():
        value = str(value).strip() if pd.notna(value) else ""

        if value == "":  # Empty field, pass as-is
            parsed[field] = value
            continue

        try:
            if field == "request_date":
                parsed[field] = pd.to_datetime(value)
            elif "phone" in field:
                parsed[field] = parse_phone_number(value)
            elif field in [
                "share_customer_data", "accepts_returns", "shipping_us_fee_enabled",
                "shipping_us_free_threshold_enabled", "shipping_intl_fee_enabled",
                "shipping_intl_free_threshold_enabled", "partner_request_is_deleted"]:
                parsed[field] = parse_boolean(value)
            elif field in [
                "supplier_avg_fulfill_time", "shipping_profile_us", "shipping_us_name",
                "shipping_profile_intl", "shipping_intl_name", "vm_supplier_country",
                "vm_supplier_state", "supplier_plan_type", "supplier_plan_name",
                "supplier_plan_status", "supplier_account_flag", "supplier_carro_environment",
                "supplier_shopify_plan_name"]:
                parsed[field] = value
            elif field in [
                "supplier_cut", "retailer_cut", "carro_cut", "default_supplier_cut",
                "shipping_us_price", "shipping_us_free_threshold", "shipping_intl_price",
                "shipping_intl_free_threshold", "shipping_us_free_threshold_name",
                "shipping_intl_free_threshold_name", "vm_supplier_zip"]:
                parsed[field] = float(value) if value else 0
            else:
                parsed[field] = value

        except Exception as e:
            raise ValueError(f"Field '{field}' failed with error: {str(e)}")

    return parsed

# === Firestore Upload ===
def upload_to_firestore(records):
    success_count = 0
    failed_records = []

    for i in tqdm(range(0, len(records), BATCH_SIZE), desc="Uploading"):
        batch = db.batch()
        batch_records = records[i:i+BATCH_SIZE]

        for record in batch_records:
            try:
                parsed = validate_and_parse(record)
                doc_ref = db.collection(COLLECTION_NAME).document(parsed["supplier_id"])
                batch.set(doc_ref, parsed)
                success_count += 1
            except Exception as e:
                record_copy = record.copy()
                record_copy["error"] = str(e)
                failed_records.append(record_copy)

        try:
            batch.commit()
        except Exception as e:
            for r in batch_records:
                failed_records.append({
                    **r,
                    "error": f"Batch commit failed: {str(e)}"
                })
            success_count -= len(batch_records)

    return success_count, failed_records

# === Main Process ===
def main():
    df = pd.read_excel(EXCEL_PATH, dtype=str)
    records = df.to_dict(orient="records")
    initial_count = len(records)

    success_count, failed_records = upload_to_firestore(records)

    if failed_records:
        pd.DataFrame(failed_records).to_excel(FAILED_OUTPUT_PATH, index=False)

    print("\n=== Upload Summary ===")
    print(f"Total Records         : {initial_count}")
    print(f"Successfully Uploaded : {success_count}")
    print(f"Failed Records        : {len(failed_records)}")
    print(f"Error Log File        : {FAILED_OUTPUT_PATH if failed_records else 'None'}")

if __name__ == "__main__":
    main()
