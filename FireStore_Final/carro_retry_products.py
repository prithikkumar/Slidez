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
MISSING_JSONL = "Raw_Data/missing_products.jsonl"
OUTPUT_JSONL = "Raw_Data/fetched_products.jsonl"
RETRY_FAILED_JSONL = "Raw_Data/missing_products_retry.jsonl"
SUMMARY_LOG = "Raw_Data/retry_summary.log"
BATCH_SIZE = 25
MAX_RETRIES = 2

GRAPHQL_URL = 'https://api.getcarro.com/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer YOUR_TOKEN_HERE'  # 🔁 Replace with actual token
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

# === Logger ===
logging.basicConfig(
    filename=SUMMARY_LOG,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === JSONL Utilities ===
async def write_jsonl(records, path):
    async with aiofiles.open(path, 'a', encoding='utf-8') as f:
        for rec in records:
            await f.write(json.dumps(rec, ensure_ascii=False) + '\n')

async def read_missing_ids():
    ids = []
    async with aiofiles.open(MISSING_JSONL, 'r', encoding='utf-8') as f:
        async for line in f:
            try:
                data = json.loads(line)
                pid = data.get("product_id")
                if pid:
                    ids.append(pid)
            except:
                continue
    return ids

# === GraphQL Fetch ===
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

# === Retry Logic ===
async def retry_missing_products():
    retry_ids = await read_missing_ids()
    success_records = []
    failed_records = []
    retries = defaultdict(int)

    async with aiohttp.ClientSession() as session:
        pending_ids = list(set(retry_ids))
        pbar = tqdm(total=len(pending_ids), desc="🔁 Retrying Missing", ncols=100)

        while pending_ids:
            current_batch = pending_ids[:BATCH_SIZE]
            pending_ids = pending_ids[BATCH_SIZE:]

            result = await fetch_batch(session, current_batch)

            if "error" in result:
                error = result["error"]
                for pid in current_batch:
                    retries[pid] += 1
                    if retries[pid] <= MAX_RETRIES:
                        pending_ids.append(pid)
                    else:
                        failed_records.append({
                            "product_id": pid,
                            "reason": error,
                            "timestamp": datetime.utcnow().isoformat()
                        })
            elif "errors" in result:
                error = result["errors"][0].get("message", "GraphQL error")
                for pid in current_batch:
                    retries[pid] += 1
                    if retries[pid] <= MAX_RETRIES:
                        pending_ids.append(pid)
                    else:
                        failed_records.append({
                            "product_id": pid,
                            "reason": error,
                            "timestamp": datetime.utcnow().isoformat()
                        })
            else:
                edges = result.get("data", {}).get("products", {}).get("edges", [])
                found_ids = set()
                for edge in edges:
                    success_records.append(edge["node"])
                    found_ids.add(edge["node"]["id"])

                for pid in current_batch:
                    if pid not in found_ids:
                        retries[pid] += 1
                        if retries[pid] <= MAX_RETRIES:
                            pending_ids.append(pid)
                        else:
                            failed_records.append({
                                "product_id": pid,
                                "reason": "Not returned in edges",
                                "timestamp": datetime.utcnow().isoformat()
                            })

            pbar.update(len(current_batch))
        pbar.close()

    await write_jsonl(success_records, OUTPUT_JSONL)
    await write_jsonl(failed_records, RETRY_FAILED_JSONL)

    logging.info("=== 🔁 Retry Summary ===")
    logging.info(f"Total retry attempted: {len(retry_ids)}")
    logging.info(f"Successfully fetched:  {len(success_records)}")
    logging.info(f"Still missing:         {len(failed_records)}")
    logging.info(f"✅ Appended to:         {OUTPUT_JSONL}")
    logging.info(f"⚠️  Remaining in:        {RETRY_FAILED_JSONL}")

# === Entrypoint ===
if __name__ == "__main__":
    asyncio.run(retry_missing_products())
    print("✅ Retry complete.")
