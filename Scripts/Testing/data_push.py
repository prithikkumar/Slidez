import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json
import re
import logging
from concurrent.futures import ThreadPoolExecutor
import gc
from tqdm import tqdm
import ast

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Firebase
cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\slidez_important.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# === Helper Functions ===

def parse_dict_from_string(raw_value):
    try:
        if pd.isna(raw_value) or not isinstance(raw_value, str):
            return {}
        return ast.literal_eval(raw_value)
    except Exception as e:
        logging.error(f"Failed parsing dict string: {raw_value} | Error: {e}")
        return {}

def parse_available_options(raw):
    try:
        if pd.isna(raw) or str(raw).strip() == '':
            return []

        val = str(raw)

        # Remove 'array([...])' pattern entirely
        val = re.sub(r"array\((\[.*?\])(?:,\s*dtype=[^)]+)?\)", r"\1", val)

        parsed = ast.literal_eval(val)

        for option in parsed:
            if isinstance(option.get('values'), list):
                option['values'] = [str(v) for v in option['values']]
        return parsed
    except Exception as e:
        logging.error(f"Failed parsing availableOptions: {raw} | Error: {e}")
        return []

def parse_categories(raw_value):
    if isinstance(raw_value, list):
        return raw_value
    if pd.isna(raw_value) or not isinstance(raw_value, str) or not raw_value.strip():
        return []
    try:
        cleaned = raw_value.replace("->", ",").strip("[]")
        return [c.strip() for c in cleaned.split(',') if c.strip()]
    except Exception as e:
        logging.error(f"Failed parsing categories: {raw_value} | Error: {e}")
        return []

def parse_images_from_string(raw_value):
    if pd.isna(raw_value) or not isinstance(raw_value, str):
        return []
    try:
        cleaned_string = raw_value.replace("'", "\"").replace(": None", ": null")
        cleaned_string = re.sub(r"(?<=\})(?=\s*\{)", ",", cleaned_string)
        images_list = json.loads(cleaned_string)
        return [{
            'altText': img.get('altText', ''),
            'id': img.get('id', ''),
            'mediaURL': img.get('mediaURL', ''),
            'position': int(img.get('position', 0))
        } for img in images_list]
    except Exception as e:
        logging.error(f"Failed parsing images: {raw_value} | Error: {e}")
        return []

def create_firestore_item(item):
    firestore_item = {}

    # Required fields
    firestore_item['id'] = item.get('id')
    firestore_item['title'] = item.get('title')
    firestore_item['descriptionHtml'] = item.get('descriptionHtml')
    firestore_item['totalVariants'] = int(item['totalVariants']) if not pd.isna(item.get('totalVariants')) else 0
    firestore_item['supplierName'] = item.get('supplierName')
    firestore_item['brandDescription'] = item.get('brandDescription')
    firestore_item['description'] = item.get('description')

    # Special fields
    firestore_item['categories'] = parse_categories(item.get('categories'))
    firestore_item['availableOptions'] = parse_available_options(item.get('availableOptions', ''))
    firestore_item['featuredImage'] = parse_dict_from_string(item.get('featuredImage'))
    firestore_item['featuredMedia'] = parse_dict_from_string(item.get('featuredMedia'))
    firestore_item['images'] = parse_images_from_string(item.get('images'))

    # Date conversion
    updated_at = item.get('updatedAt')
    if pd.notna(updated_at):
        try:
            firestore_item['updatedAt'] = pd.to_datetime(updated_at)
        except Exception:
            firestore_item['updatedAt'] = None
    else:
        firestore_item['updatedAt'] = None

    return firestore_item

# === Load Excel ===

df = pd.read_excel(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\output_file.xlsx', engine='openpyxl')
data = df.to_dict(orient='records')
logging.info(f"Total records in file: {len(data)}")

# === Push Single Record ===

target_id = 'a08tzwq02r0exwqu3m50b6iu'
record = next((item for item in data if item['id'] == target_id), None)

if record:
    try:
        doc_data = create_firestore_item(record)
        db.collection('products').document(target_id).set(doc_data)
        logging.info(f"✅ Successfully pushed record with ID: {target_id}")
    except Exception as e:
        logging.error(f"❌ Failed to push record ID {target_id}: {e}")
else:
    logging.warning(f"❌ No record found with ID: {target_id}")

gc.collect()
