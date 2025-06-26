import pandas as pd
import aiohttp
import asyncio
import aiofiles
import json
import os
import logging
from tqdm.asyncio import tqdm
from datetime import datetime
from collections import defaultdict

# === Config ===
INPUT_XLSX = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Raw_Data_Input\Slidez_products_excel.csv'
OUTPUT_JSONL = "Raw_Data/fetched_products.jsonl"
MISSING_JSONL = "Raw_Data/missing_products.jsonl"
SUMMARY_LOG = "Raw_Data/fetch_summary.log"
BATCH_SIZE = 25
MAX_RETRIES = 3
CONCURRENT_WORKERS = 10

GRAPHQL_URL = 'https://api.getcarro.com/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im5JNVZmby1SVG5nMWNNUm9uZ0t2VSJ9.eyJtb2duZXQvYXNzb2NpYXRlZF9icmFuZF9pZCI6Im1reDBsMWprejIxdHNzNTRobnE1NHo0ZSIsImlzcyI6Imh0dHBzOi8vdnlybC51cy5hdXRoMC5jb20vIiwic3ViIjoiNHJVbFR5MkJVYnNXNTAxOVdRVW43Q3hyZDlLZUR1SDBAY2xpZW50cyIsImF1ZCI6InVybjpjYXJyby1wbGF0Zm9ybSIsImlhdCI6MTc0OTAwMzk0MSwiZXhwIjoxNzUxNTk1OTQxLCJzY29wZSI6InJlYWQ6ZGlyZWN0b3J5IHJlYWQ6cHJvZHVjdHMgd3JpdGU6cHJvZHVjdHMgcmVhZDpkb2NzIGNyZWF0ZTp1c2VycyByZWFkOm9yZGVycyB3cml0ZTpvcmRlcnMiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiI0clVsVHkyQlVic1c1MDE5V1FVbjdDeHJkOUtlRHVIMCIsInBlcm1pc3Npb25zIjpbInJlYWQ6ZGlyZWN0b3J5IiwicmVhZDpwcm9kdWN0cyIsIndyaXRlOnByb2R1Y3RzIiwicmVhZDpkb2NzIiwiY3JlYXRlOnVzZXJzIiwicmVhZDpvcmRlcnMiLCJ3cml0ZTpvcmRlcnMiXX0.ie7LB6n7TwBl0yzpK6B6NpP-Vc8B6_X4k6xPogz1XhHXbIuuv0m3wcJ1hOaSwH1Hkkn3FZVOUvzgdh-_KHzwjgtfunQu7s0HQ7-5NJxwU7iC-cGcJSnh4gDoHDGNbAnyYI7EXVNbIUSiRW8EQucYGwJdDsOE3d1StvHInRo5_nqdpSq_g6OeOfc3k8X7Ja5tYWR9XgnlEM3KUi0o5mPbx_7dkBC_3hH3_TPONZ6fg78V9AGsh9Qg7QHH9haCVqK2CxC1LEOjeItgGN4gV4BPLeEG6coyq9IzDBKYsfNyeecfdQzoWDCFGJ6vdMUf5vRbFPmuo3A67LggBKG8Fu1ZVw'  # Replace with actual token
}

GRAPHQL_QUERY = """query ($productIds: [ID!], $first: Int!) {
  products(productIds: $productIds, first: $first) {
    edges {
      node {
        id
        title
        descriptionHtml
        totalVariants
        supplierId 
        supplierName
        images {
          altText
          id
          mediaURL
          position
        }
        availableOptions {
          name
          position
          values
        }
        categories
        featuredImage {
          altText
          id
          mediaURL
        }
        featuredMedia {
          altText
          id
          mediaURL
        }
        brandDescription
        description
        updatedAt
      }
    }
  }
}"""

os.makedirs(os.path.dirname(OUTPUT_JSONL), exist_ok=True)

# === Logging Setup ===
logging.basicConfig(
    filename=SUMMARY_LOG,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === JSONL Writer ===
async def write_jsonl(records, path):
    async with aiofiles.open(path, 'a', encoding='utf-8') as f:
        for rec in records:
            await f.write(json.dumps(rec, ensure_ascii=False) + '\n')

# === GraphQL Fetch Function ===
async def fetch_batch(session, batch):
    payload = {
        'query': GRAPHQL_QUERY,
        'variables': {
            'productIds': batch,
            'first': min(len(batch), 25)
        }
    }
    try:
        async with session.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=60) as resp:
            return await resp.json()
    except Exception as e:
        return {"error": str(e)}

# === Worker ===
async def worker(queue, session, success_records, missing_records, retries, processed_ids, pbar):
    while True:
        try:
            batch = await queue.get()
        except asyncio.CancelledError:
            break

        result = await fetch_batch(session, batch)
        found_ids = set()

        if "error" in result:
            error_msg = result["error"]
            for pid in batch:
                if pid not in processed_ids:
                    retries[pid] += 1
                    if retries[pid] <= MAX_RETRIES:
                        await queue.put([pid])
                    else:
                        missing_records.append({
                            "product_id": pid,
                            "reason": error_msg,
                            "timestamp": datetime.utcnow().isoformat()
                        })
                        processed_ids.add(pid)
                        pbar.update(1)

        elif "errors" in result:
            error_msg = result["errors"][0].get("message", "GraphQL error")
            for pid in batch:
                if pid not in processed_ids:
                    retries[pid] += 1
                    if retries[pid] <= MAX_RETRIES:
                        await queue.put([pid])
                    else:
                        missing_records.append({
                            "product_id": pid,
                            "reason": error_msg,
                            "timestamp": datetime.utcnow().isoformat()
                        })
                        processed_ids.add(pid)
                        pbar.update(1)

        else:
            edges = result.get("data", {}).get("products", {}).get("edges", [])
            for edge in edges:
                product = edge["node"]
                pid = product["id"]
                if pid not in processed_ids:
                    success_records.append(product)
                    processed_ids.add(pid)
                    pbar.update(1)
                    found_ids.add(pid)

            for pid in batch:
                if pid not in found_ids and pid not in processed_ids:
                    retries[pid] += 1
                    if retries[pid] <= MAX_RETRIES:
                        await queue.put([pid])
                    else:
                        missing_records.append({
                            "product_id": pid,
                            "reason": "Not returned in edges",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                        processed_ids.add(pid)
                        pbar.update(1)

        queue.task_done()

# === Main Fetch Logic ===
async def fetch_all_products(product_ids):
    queue = asyncio.Queue()
    success_records = []
    missing_records = []
    retries = defaultdict(int)
    processed_ids = set()

    unique_ids = list(set(product_ids))
    for i in range(0, len(unique_ids), BATCH_SIZE):
        await queue.put(unique_ids[i:i+BATCH_SIZE])

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=len(unique_ids), desc="📦 Fetching Products", ncols=100)
        workers = [
            asyncio.create_task(worker(queue, session, success_records, missing_records, retries, processed_ids, pbar))
            for _ in range(CONCURRENT_WORKERS)
        ]
        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        pbar.close()

    await write_jsonl(success_records, OUTPUT_JSONL)
    await write_jsonl(missing_records, MISSING_JSONL)

    logging.info("=== ✅ Fetch Summary ===")
    logging.info(f"Total requested:      {len(unique_ids)}")
    logging.info(f"Successfully fetched: {len(success_records)}")
    logging.info(f"Missing / Failed:     {len(missing_records)}")
    logging.info(f"✅ Saved to:           {OUTPUT_JSONL}")
    logging.info(f"⚠️  Missing saved to:  {MISSING_JSONL}")

# === Entrypoint ===
def main():
    try:
        df = pd.read_csv(INPUT_XLSX)
        df.columns = df.columns.str.strip().str.lower()
        if "retailer_product_id" not in df.columns:
            raise ValueError("Missing 'retailer_product_id' column")

        product_ids = df['retailer_product_id'].dropna().astype(str).unique().tolist()
        asyncio.run(fetch_all_products(product_ids))

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
