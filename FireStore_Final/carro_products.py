import pandas as pd
import asyncio
import aiohttp
import os
import json
import aiofiles
from tqdm.asyncio import tqdm
from collections import defaultdict, deque
from datetime import datetime

# === Config ===
INPUT_FILE = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\FireStore_Final\invalid_products_20250607_121344.xlsx'
OUTPUT_JSONL = "Raw_Data/products.jsonl"
MISSING_JSONL = "Raw_Data/missing_products.jsonl"
INCOMPLETE_JSON = "Raw_Data/incomplete_products.json"
BATCH_SIZE = 100
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 20
MAX_PRODUCTS = 99

GRAPHQL_URL = 'https://api.getcarro.com/graphql'
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

HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im5JNVZmby1SVG5nMWNNUm9uZ0t2VSJ9.eyJtb2duZXQvYXNzb2NpYXRlZF9icmFuZF9pZCI6Im1reDBsMWprejIxdHNzNTRobnE1NHo0ZSIsImlzcyI6Imh0dHBzOi8vdnlybC51cy5hdXRoMC5jb20vIiwic3ViIjoiNHJVbFR5MkJVYnNXNTAxOVdRVW43Q3hyZDlLZUR1SDBAY2xpZW50cyIsImF1ZCI6InVybjpjYXJyby1wbGF0Zm9ybSIsImlhdCI6MTc0OTAwMzk0MSwiZXhwIjoxNzUxNTk1OTQxLCJzY29wZSI6InJlYWQ6ZGlyZWN0b3J5IHJlYWQ6cHJvZHVjdHMgd3JpdGU6cHJvZHVjdHMgcmVhZDpkb2NzIGNyZWF0ZTp1c2VycyByZWFkOm9yZGVycyB3cml0ZTpvcmRlcnMiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiI0clVsVHkyQlVic1c1MDE5V1FVbjdDeHJkOUtlRHVIMCIsInBlcm1pc3Npb25zIjpbInJlYWQ6ZGlyZWN0b3J5IiwicmVhZDpwcm9kdWN0cyIsIndyaXRlOnByb2R1Y3RzIiwicmVhZDpkb2NzIiwiY3JlYXRlOnVzZXJzIiwicmVhZDpvcmRlcnMiLCJ3cml0ZTpvcmRlcnMiXX0.ie7LB6n7TwBl0yzpK6B6NpP-Vc8B6_X4k6xPogz1XhHXbIuuv0m3wcJ1hOaSwH1Hkkn3FZVOUvzgdh-_KHzwjgtfunQu7s0HQ7-5NJxwU7iC-cGcJSnh4gDoHDGNbAnyYI7EXVNbIUSiRW8EQucYGwJdDsOE3d1StvHInRo5_nqdpSq_g6OeOfc3k8X7Ja5tYWR9XgnlEM3KUi0o5mPbx_7dkBC_3hH3_TPONZ6fg78V9AGsh9Qg7QHH9haCVqK2CxC1LEOjeItgGN4gV4BPLeEG6coyq9IzDBKYsfNyeecfdQzoWDCFGJ6vdMUf5vRbFPmuo3A67LggBKG8Fu1ZVw'
}

os.makedirs(os.path.dirname(OUTPUT_JSONL), exist_ok=True)

# === Dynamic Missing Field Checker ===
def find_missing_fields(data, prefix=''):
    missing_fields = []
    missing_values = {}
    if isinstance(data, dict):
        for k, v in data.items():
            path = f"{prefix}.{k}" if prefix else k
            if v in [None, '', [], {}, float('nan')]:
                missing_fields.append(path)
                # Save only the missing value
                missing_values.setdefault(prefix, {})[k] = v
            elif isinstance(v, (dict, list)):
                sub_fields, sub_values = find_missing_fields(v, path)
                missing_fields.extend(sub_fields)
                if sub_values:
                    missing_values.setdefault(k, {}).update(sub_values)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            path = f"{prefix}[{i}]"
            if item in [None, '', [], {}, float('nan')]:
                missing_fields.append(path)
                missing_values[path] = item
            elif isinstance(item, (dict, list)):
                sub_fields, sub_values = find_missing_fields(item, path)
                missing_fields.extend(sub_fields)
                if sub_values:
                    missing_values.setdefault(path, {}).update(sub_values)
    return missing_fields, missing_values

# === Flush JSONL ===
async def flush_to_jsonl(buffer, path, lock):
    async with lock:
        if not buffer:
            return
        async with aiofiles.open(path, 'a', encoding='utf-8') as f:
            while buffer:
                await f.write(json.dumps(buffer.popleft(), ensure_ascii=False) + '\n')

# === Flush JSON ===
async def flush_to_json(buffer, path, lock):
    async with lock:
        if not buffer:
            return
        async with aiofiles.open(path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(buffer, ensure_ascii=False, indent=2))

# === Fetch Product ===
async def fetch_product(session, product_id):
    payload = {
        'query': GRAPHQL_QUERY,
        'variables': {'productIds': [product_id], 'first': 1}
    }
    try:
        async with session.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=20) as response:
            result = await response.json()
            if 'errors' in result:
                return None, f"GraphQL Error: {result['errors'][0].get('message', 'Unknown')}"
            edges = result.get('data', {}).get('products', {}).get('edges', [])
            if not edges:
                return None, "No product found"
            return edges[0]['node'], None
    except Exception as e:
        return None, str(e)

# === Worker Task ===
async def worker(queue, session, pbar):
    while True:
        try:
            product_id = await queue.get()
        except asyncio.CancelledError:
            break

        product, error = await fetch_product(session, product_id)

        if product:
            missing_fields, missing_values = find_missing_fields(product)

            if missing_fields:
                async with incomplete_lock:
                    incomplete_list.append({
                        "product_id": product_id,
                        "missing_fields": missing_fields,
                        "missing_values": missing_values
                    })

            async with product_lock:
                product_buffer.append(product)
                if len(product_buffer) >= BATCH_SIZE:
                    await flush_to_jsonl(product_buffer, OUTPUT_JSONL, product_lock)

        else:
            retry_counts[product_id] += 1
            if retry_counts[product_id] <= MAX_RETRIES:
                await queue.put(product_id)
            else:
                async with missing_lock:
                    missing_buffer.append({
                        "product_id": product_id,
                        "reason": error,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    if len(missing_buffer) >= BATCH_SIZE:
                        await flush_to_jsonl(missing_buffer, MISSING_JSONL, missing_lock)

        pbar.update(1)
        queue.task_done()

# === Main Runner ===
async def main():
    try:
        df = pd.read_excel(INPUT_FILE)
        df.columns = df.columns.str.strip().str.lower()
    except Exception as e:
        raise RuntimeError(f"Failed to load input file: {e}")

    if 'id' not in df.columns:
        raise ValueError("Missing 'id' column")

    product_ids = df['id'].dropna().astype(str).unique().tolist()[:MAX_PRODUCTS]

    queue = asyncio.Queue()
    for pid in product_ids:
        await queue.put(pid)

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=len(product_ids), desc="🚀 Fetching", ncols=100)
        workers = [asyncio.create_task(worker(queue, session, pbar)) for _ in range(CONCURRENT_REQUESTS)]
        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        pbar.close()

    await flush_to_jsonl(product_buffer, OUTPUT_JSONL, product_lock)
    await flush_to_jsonl(missing_buffer, MISSING_JSONL, missing_lock)
    await flush_to_json(incomplete_list, INCOMPLETE_JSON, incomplete_lock)

# === Shared Buffers and Locks ===
product_buffer = deque()
missing_buffer = deque()
incomplete_list = []
product_lock = asyncio.Lock()
missing_lock = asyncio.Lock()
incomplete_lock = asyncio.Lock()
retry_counts = defaultdict(int)

# === Entrypoint ===
if __name__ == "__main__":
    asyncio.run(main())
    print("✅ All tasks completed.")
