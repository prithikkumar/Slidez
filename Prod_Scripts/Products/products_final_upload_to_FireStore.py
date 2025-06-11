import pandas as pd
import re
import ast
import logging
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Initialize Firestore ===
cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Prod_Scripts\slidez_important.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# === Logging setup ===
logging.basicConfig(filename='firestore_upload_summary.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# === Parsing Helpers ===
def fix_syntax_issues(raw, handle_dtype=True):
    """
    General function to fix common issues in the raw string:
    - dtype=object patterns (e.g., from numpy arrays)
    - missing commas between dicts
    """
    try:
        val = str(raw)

        if handle_dtype and 'dtype=' in val:
            # Remove array(...) wrapper and dtype
            val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)

        # Add missing commas between dicts
        val = re.sub(r'}\s*{', '}, {', val)

        return ast.literal_eval(val)
    except Exception as e:
        logging.error(f"Fixing syntax failed for: {raw} | Error: {e}")
        return None

def fix_invalid_syntax(raw):
    return fix_syntax_issues(raw, handle_dtype=True)

def fix_malformed_node(raw):
    return fix_syntax_issues(raw, handle_dtype=False)

def fix_delimiter_issues(raw):
    return fix_syntax_issues(raw, handle_dtype=True)

def smart_parse(raw, error):
    """
    Chooses the appropriate fix based on the type of error.
    """
    error = str(error).lower()
    raw_str = str(raw).lower()

    logging.info(f"Smart parsing triggered for error: {error}")

    if 'invalid syntax' in error:
        return fix_invalid_syntax(raw)
    elif 'malformed node or string' in error and 'ast.call' in error:
        return fix_malformed_node(raw)
    elif 'dtype=object' in raw_str:
        return fix_delimiter_issues(raw)

    logging.warning(f"No known fix for availableOptions: {error}")
    return None

def normalize_options(options):
    """
    Ensures each 'values' list in availableOptions is a list of strings.
    """
    if not isinstance(options, list):
        return options

    for option in options:
        if isinstance(option.get('values'), list):
            option['values'] = [str(v).strip() for v in option['values']]
    return options

def parse_available_options(available_options_str):
    """
    Main function to parse availableOptions from raw string, including error correction.
    """
    try:
        parsed = ast.literal_eval(available_options_str)
        return normalize_options(parsed)
    except Exception as e:
        logging.warning(f"Initial literal_eval failed: {e}")
        fixed = smart_parse(available_options_str, e)
        if fixed is not None:
            return normalize_options(fixed)
        return None

def safe_parse_literal(val):
    try:
        val = str(val)
        val = re.sub(r'^\\', '', val)  # remove starting backslashes
        val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)  # fix numpy arrays
        return ast.literal_eval(val)
    except Exception:
        return None

def parse_featured_media(val):
    try:
        return ast.literal_eval(val) if isinstance(val, str) else val
    except Exception:
        return None

def parse_images(images_str: str):
    try:
        # Fix missing commas between dicts
        fixed_str = re.sub(r"\}(?=\s*\{)", "},", images_str.strip())

        # Handle escaped brackets from Excel export
        fixed_str = fixed_str.strip().replace("\\[", "[").replace("\\]", "]")

        # Evaluate to Python list of dicts
        images = ast.literal_eval(fixed_str)

        # Ensure it's a list of dicts with required keys
        if isinstance(images, list) and all(isinstance(item, dict) for item in images):
            return images
    except Exception:
        pass
    return []

def parse_categories(categories_str: str):
    try:
        # Convert to list (e.g., from "['A->B->C']" to ['A->B->C'])
        category_list = ast.literal_eval(categories_str)
        if isinstance(category_list, list) and category_list:
            # Split the first (and only) item on '->'
            return category_list[0].split("->")
    except Exception:
        pass
    return []


def parse_updated_at(val):
    try:
        return datetime.strptime(str(val), '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

# === Upload Batch ===
def upload_batch(batch_records, collection_name="products"):
    batch = db.batch()
    for record in batch_records:
        doc_ref = db.collection(collection_name).document(record['id'])
        data = record.copy()
        batch.set(doc_ref, data)
    batch.commit()

# === Parallel Processing ===
def process_records_in_parallel(records, batch_size=400, max_workers=4, collection_name="products"):
    total = len(records)
    batches = [records[i:i + batch_size] for i in range(0, total, batch_size)]
    summary = {"total": total, "success": 0, "failed": 0}
    failed_records = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(upload_batch, batch, collection_name): batch for batch in batches}
        for future in tqdm(as_completed(future_to_batch), total=len(future_to_batch), desc="Uploading"):
            batch = future_to_batch[future]
            try:
                future.result()
                summary["success"] += len(batch)
            except Exception as e:
                summary["failed"] += len(batch)
                failed_records.extend(batch)

    return summary, failed_records

# === Main ===
def main():
    file_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\products_output.xlsx"
    df = pd.read_excel(file_path)
    records_to_push = []

    for _, row in df.iterrows():
        doc = row.to_dict()
        doc_id = str(doc.pop("id", None))
        if not doc_id:
            continue

        # Parse complex fields
        doc["featuredImage"] = parse_featured_media(doc.get("featuredImage"))
        doc["featuredMedia"] = parse_featured_media(doc.get("featuredMedia"))
        doc["images"] = parse_images(doc.get("images"))
        # doc["categories"] = parse_categories(doc.get("categories"))
        doc["updatedAt"] = parse_updated_at(doc.get("updatedAt"))

        #Parsing Available Options
        # Extract the raw string from the Excel row
        available_options_str = str(row.get("availableOptions", "")).strip()
        parsed_options = None

        try:
            # First, try to parse it normally
            parsed_options = parse_available_options(available_options_str)
        except Exception as e:
            logging.error(f"Initial parse error: {e}")
            parsed_options = None

        # If it's still a string or failed to parse, try to fix it using known error types
        if parsed_options is None or isinstance(parsed_options, str):
            for err_type in [
                "malformed node or string on line 1: <ast.Call object",
                "invalid syntax",
                "dtype=object"
            ]:
                parsed_options = smart_parse(available_options_str, err_type)
                if parsed_options is not None:
                    break

        # Normalize if we got something back, or else default to empty list
        doc["availableOptions"] = normalize_options(parsed_options if parsed_options else [])



        records_to_push.append({
            "id": doc_id,
            **doc
        })

    if records_to_push:
        print(f"\n📤 Uploading {len(records_to_push)} records to Firestore...\n")
        summary, failed_records = process_records_in_parallel(records_to_push)
    else:
        summary = {"total": 0, "success": 0, "failed": 0}
        failed_records = []

    # === Save Failed Records ===
    if failed_records:
        failed_df = pd.DataFrame(failed_records)
        failed_path = file_path.replace(".xlsx", "_failed_uploads.xlsx")
        failed_df.to_excel(failed_path, index=False)
        print(f"\n⚠️ Saved {len(failed_records)} failed records to '{failed_path}'")

    # === Summary ===
    print("\n=== Upload Summary ===")
    print(f"Total Records:  {summary['total']}")
    print(f"Success:        {summary['success']}")
    print(f"Failed:         {summary['failed']}")

    logging.info("Upload Summary: %s", summary)

if __name__ == "__main__":
    main()
