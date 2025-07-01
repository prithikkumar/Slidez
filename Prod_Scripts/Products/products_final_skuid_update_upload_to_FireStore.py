import pandas as pd
import firebase_admin
import logging
from firebase_admin import credentials, firestore
from tqdm import tqdm
import time
import os
import json

# === Firestore Setup ===
if not firebase_admin._apps:
    cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Prod_Scripts\slidez_important.json')
    firebase_admin.initialize_app(cred)
db = firestore.client()

# === Logging Setup ===
logging.basicConfig(
    filename="update_sku_id.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Firestore Batch Update Task ===
def batch_update_skus(batch_records, batch_number):
    batch = db.batch()
    failed = []

    for record in batch_records:
        product_name = str(record.get("Product Name*", "")).strip()
        sku_id_raw = record.get("SKU Id", "")

        if not product_name or not sku_id_raw:
            failed.append({
                "Product Name*": product_name,
                "SKU Id": sku_id_raw,
                "error": "Missing name or SKU"
            })
            continue

        try:
            sku_id = json.loads(str(sku_id_raw).strip())
            if not isinstance(sku_id, dict):
                raise ValueError("Parsed SKU Id is not a dictionary")
        except Exception as e:
            failed.append({
                "Product Name*": product_name,
                "SKU Id": sku_id_raw,
                "error": f"Invalid dictionary JSON: {e}"
            })
            continue

        try:
            docs = list(db.collection("products").where("title", "==", product_name).stream())

            if not docs:
                failed.append({
                    "Product Name*": product_name,
                    "SKU Id": sku_id_raw,
                    "error": "No document found"
                })
                continue

            doc_ref = docs[0].reference
            batch.update(doc_ref, {"SKU Id": sku_id})

        except Exception as e:
            logging.error(f"❌ Firestore update failed for '{product_name}': {e}")
            failed.append({
                "Product Name*": product_name,
                "SKU Id": sku_id_raw,
                "error": str(e)
            })

    try:
        batch.commit()
        return {"success": len(batch_records) - len(failed), "failed": failed}
    except Exception as e:
        logging.error(f"🔥 Firestore batch commit failed for batch {batch_number}: {e}")
        for rec in batch_records:
            rec["error"] = f"Batch commit error: {str(e)}"
        return {"success": 0, "failed": batch_records}


# === Main Function ===
def main():
    file_path = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\SKU Ids.xlsx'
    batch_size = 500

    df = pd.read_excel(file_path, engine="openpyxl")
    df = df[["Product Name*", "SKU Id"]].dropna(subset=["Product Name*", "SKU Id"])
    df["Product Name*"] = df["Product Name*"].str.strip()
    records = df.to_dict(orient="records")

    total_batches = (len(records) + batch_size - 1) // batch_size
    success_total = 0
    failed_all = []

    progress_file = file_path.replace(".xlsx", "_sku_update_progress.csv")

    for i in tqdm(range(total_batches), desc="🔁 Processing Batches"):
        batch_records = records[i * batch_size : (i + 1) * batch_size]
        result = batch_update_skus(batch_records, i + 1)

        success_total += result["success"]
        failed_all.extend(result["failed"])

        # Save progress
        with open(progress_file, "a", encoding="utf-8") as f:
            for rec in batch_records:
                f.write(f"{rec.get('Product Name*','')},{rec.get('SKU Id','')}\n")

        time.sleep(0.5)  # slight pause to prevent throttling

    print("\n=== Firestore SKU ID Update Summary ===")
    print(f"Total Records: {len(records)}")
    print(f"Successful:    {success_total}")
    print(f"Failed:        {len(failed_all)}")

    if failed_all:
        failed_df = pd.DataFrame(failed_all)
        failed_file = file_path.replace(".xlsx", "_sku_id_update_failed.xlsx")
        failed_df.to_excel(failed_file, index=False)
        print(f"⚠️ {len(failed_all)} failed updates saved to '{failed_file}'")

    print(f"✅ Progress saved to '{progress_file}'")


# === Entry Point ===
if __name__ == "__main__":
    main()
