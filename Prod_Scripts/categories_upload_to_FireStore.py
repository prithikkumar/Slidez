import pandas as pd
import ast
from collections import defaultdict
from firebase_admin import credentials, firestore, initialize_app
from tqdm import tqdm

# --- Step 1: Load Excel ---
df = pd.read_excel(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\categories_slidez.xlsx")

# Normalize column names to lowercase with underscores
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Check required columns exist
if not {'category', 'sub_categories'}.issubset(df.columns):
    raise ValueError("Excel must contain 'category' and 'sub_categories' columns.")

# --- Step 2: Build the category tree dictionary ---
cat_tree = defaultdict(set)

for _, row in df.iterrows():
    category = row["category"]
    sub_cat_raw = row["sub_categories"]

    # Parse sub_categories string into list if needed
    if isinstance(sub_cat_raw, str) and sub_cat_raw.strip().startswith("["):
        try:
            sub_cats = ast.literal_eval(sub_cat_raw)
            if isinstance(sub_cats, list):
                for sc in sub_cats:
                    cat_tree[category].add(sc)
            else:
                cat_tree[category].add(sub_cat_raw)
        except Exception:
            cat_tree[category].add(sub_cat_raw)
    else:
        cat_tree[category].add(sub_cat_raw)

# Convert sets to lists
cat_tree = {k: list(v) for k, v in cat_tree.items()}

# --- Step 3: Sanitize Firestore doc IDs ---
def sanitize_firestore_id(text):
    return text.replace("/", "_").replace(".", "_")

# --- Step 4: Initialize Firestore ---
cred = credentials.Certificate(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Prod_Scripts\slidez_important.json")
initialize_app(cred)
db = firestore.client()

# --- Step 5: Upload to Firestore with status bar ---
success_count = 0
error_count = 0

print("Pushing records to Firestore...\n")

for category in tqdm(cat_tree.keys(), desc="Uploading", unit="category"):
    sub_categories = cat_tree[category]
    doc_id = sanitize_firestore_id(category)

    try:
        db.collection("categories").document(doc_id).set({
            "category": category,
            "sub_categories": sub_categories
        })
        success_count += 1
    except Exception as e:
        print(f"\n[ERROR] Failed to push category: {category}\n{e}")
        error_count += 1

# --- Step 6: Summary ---
print("\n--- Upload Summary ---")
print(f"Total Records Attempted: {len(cat_tree)}")
print(f"Successfully Pushed:     {success_count}")
print(f"Failed to Push:          {error_count}")
