import pandas as pd
import re
import ast
import firebase_admin
from firebase_admin import credentials, firestore
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Initialize Firestore ===
cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\slidez_important.json')  # Replace this
firebase_admin.initialize_app(cred)
db = firestore.client()

# === Fix functions ===
def fix_invalid_syntax(raw):
    try:
        val = str(raw)
        val = re.sub(r'}\s*{', '}, {', val)
        val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)
        return ast.literal_eval(val)
    except Exception:
        return None

def fix_malformed_node(raw):
    try:
        val = str(raw)
        val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)
        val = re.sub(r'}\s*{', '}, {', val)
        return ast.literal_eval(val)
    except Exception:
        return None

def fix_delimiter_issues(raw):
    try:
        val = str(raw)
        if 'dtype=object' in val:
            val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)
            val = re.sub(r'}\s*{', '}, {', val)
        return ast.literal_eval(val)
    except Exception:
        return None

def smart_parse(raw, error):
    error = str(error).lower()
    if 'invalid syntax' in error:
        return fix_invalid_syntax(raw)
    elif 'malformed node or string on line 1:' in error and 'ast.call' in error:
        return fix_malformed_node(raw)
    elif 'dtype=object' in str(raw).lower():
        return fix_delimiter_issues(raw)
    return None

def normalize_options(options):
    for option in options:
        if isinstance(option.get('values'), list):
            option['values'] = [str(v) for v in option['values']]
    return options

# === Upload batch ===
def upload_batch(batch_records, collection_name="products"):
    batch = db.batch()
    for record in batch_records:
        doc_ref = db.collection(collection_name).document(record['id'])
        batch.update(doc_ref, {"availableOptions": record["availableOptions"]})
    batch.commit()

# === Parallel processing ===
def process_records_in_parallel(records, batch_size=400, max_workers=4):
    total = len(records)
    batches = [records[i:i + batch_size] for i in range(0, total, batch_size)]
    summary = {"total": total, "success": 0, "failed": 0}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_batch, batch) for batch in batches]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Uploading"):
            try:
                f.result()
                summary["success"] += batch_size
            except:
                summary["failed"] += batch_size

    return summary

# === Main function ===
def main():
    file_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\retry_failed_records.xlsx"
    df = pd.read_excel(file_path)
    records_to_push = []
    unfixed_records = []

    for _, row in df.iterrows():
        doc_id = str(row['id'])
        raw = row['raw']
        error = row['error']
        fixed = smart_parse(raw, error)
        if fixed:
            fixed = normalize_options(fixed)
            records_to_push.append({
                "id": doc_id,
                "availableOptions": fixed
            })
        else:
            unfixed_records.append({
                "id": doc_id,
                "raw": raw,
                "error": error
            })

    if records_to_push:
        print(f"\nPreparing to push {len(records_to_push)} fixed records to Firestore...\n")
        summary = process_records_in_parallel(records_to_push)
    else:
        summary = {"total": 0, "success": 0, "failed": 0}

    if unfixed_records:
        unfixed_df = pd.DataFrame(unfixed_records)
        out_path = file_path.replace(".xlsx", "_unfixed_remaining.xlsx")
        unfixed_df.to_excel(out_path, index=False)
        print(f"\n⚠️ Saved {len(unfixed_records)} unfixed records to '{out_path}'")

    print("\n=== Push Summary ===")
    print(f"Total Parsed:   {summary['total']}")
    print(f"Success:        {summary['success']}")
    print(f"Failed Uploads: {summary['failed']}")
    print(f"Unfixed Skipped: {len(unfixed_records)}")

if __name__ == "__main__":
    main()