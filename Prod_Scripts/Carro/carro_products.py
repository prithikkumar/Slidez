import pandas as pd
import asyncio
import aiohttp
import os
import time
from datetime import datetime
from tqdm import tqdm
from colorama import Fore, Style

# === Load Product IDs and Supplier Names ===
df_ids = pd.read_csv("Raw_Data/Slidez_products_excel.csv")
product_ids = df_ids['retailer_product_id'].dropna().astype(str).unique().tolist()
id_to_supplier = dict(zip(df_ids['retailer_product_id'].astype(str), df_ids['supplier_name']))
print(f"📦 Total product IDs to query: {len(product_ids)}")

# === GraphQL API Details ===
graphql_url = 'https://api.getcarro.com/graphql'
graphql_query = """query ($productIds: [ID!], $first: Int!) {
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

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im5JNVZmby1SVG5nMWNNUm9uZ0t2VSJ9.eyJtb2duZXQvYXNzb2NpYXRlZF9icmFuZF9pZCI6Im1reDBsMWprejIxdHNzNTRobnE1NHo0ZSIsImlzcyI6Imh0dHBzOi8vdnlybC51cy5hdXRoMC5jb20vIiwic3ViIjoiNHJVbFR5MkJVYnNXNTAxOVdRVW43Q3hyZDlLZUR1SDBAY2xpZW50cyIsImF1ZCI6InVybjpjYXJyby1wbGF0Zm9ybSIsImlhdCI6MTc0NDU3NjAwMywiZXhwIjoxNzQ3MTY4MDAzLCJzY29wZSI6InJlYWQ6ZGlyZWN0b3J5IHJlYWQ6cHJvZHVjdHMgd3JpdGU6cHJvZHVjdHMgcmVhZDpkb2NzIGNyZWF0ZTp1c2VycyByZWFkOm9yZGVycyB3cml0ZTpvcmRlcnMiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiI0clVsVHkyQlVic1c1MDE5V1FVbjdDeHJkOUtlRHVIMCIsInBlcm1pc3Npb25zIjpbInJlYWQ6ZGlyZWN0b3J5IiwicmVhZDpwcm9kdWN0cyIsIndyaXRlOnByb2R1Y3RzIiwicmVhZDpkb2NzIiwiY3JlYXRlOnVzZXJzIiwicmVhZDpvcmRlcnMiLCJ3cml0ZTpvcmRlcnMiXX0.bhCKskZBNKbpPP4uqIYIaWuk305OHg9TvUWLFXEIk6SvSYdvodiklwiVE_u27bo_B1McvzmEL-JC_dZvHdbQVYpfM_HEkyhNzzapBg1cpCYuOEBOjM7rnSyMBCclhgzrvoGjkWpfqZODBUSXHIy1GuknILVp-BOdAJwzs0gTOEbFqAK-QrcvVv7xDOjmp1LzS58fNyS7HBWF2wS09gsTo1TQpQ5bVEnkyIpe79g4XmvXvNCIuW0fnGjn4mVBCZ890gmeA8u-bMtK7Zx0p_B3Vzt9t3gXhpTFwKdoheaqL9XaBHRWjU_Ns5NDKPFTxRZAJupJhxKG3tivay5DxLHJVQ'  # your token here
}

# === Output Files ===
output_file = "Raw_Data/filtered_carro_products_final.csv"
missing_file = "Raw_Data/missing_product_ids.csv"

concurrent_requests = 10  # How many simultaneous product fetches (tune based on API limit)

all_products_buffer = []
missing_products_buffer = []
seen_product_ids = set()
last_write_time = time.time()

write_interval = 30  # seconds

def flush_products_to_csv():
    if all_products_buffer:
        df_temp = pd.DataFrame(all_products_buffer)
        write_mode = 'a' if os.path.exists(output_file) else 'w'
        header = not os.path.exists(output_file)
        df_temp.to_csv(output_file, mode=write_mode, index=False, header=header)
        print(Fore.GREEN + f"💾 Flushed {len(df_temp)} products at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + Style.RESET_ALL)
        all_products_buffer.clear()

def flush_missing_to_csv():
    if missing_products_buffer:
        df_temp = pd.DataFrame(missing_products_buffer)
        write_mode = 'a' if os.path.exists(missing_file) else 'w'
        header = not os.path.exists(missing_file)
        df_temp.to_csv(missing_file, mode=write_mode, index=False, header=header)
        print(Fore.RED + f"📝 Flushed {len(df_temp)} missing product logs at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + Style.RESET_ALL)
        missing_products_buffer.clear()

async def fetch_single_product(session, product_id, retries=2):
    try:
        async with session.post(graphql_url, headers=headers, json={
            'query': graphql_query,
            'variables': {
                'productIds': [product_id],
                'first': 1
            }
        }, timeout=aiohttp.ClientTimeout(total=15)) as response:
            status = response.status
            result = await response.json()

            if 'errors' in result:
                print(Fore.RED + f"❌ GraphQL error for {product_id}: {result['errors']}" + Style.RESET_ALL)
                missing_products_buffer.append({
                    'product_id': product_id,
                    'supplier_name': id_to_supplier.get(product_id),
                    'reason': f"GraphQL Error: {result['errors'][0].get('message', 'Unknown')}",
                    'http_status': status,
                    'timestamp': datetime.now().isoformat()
                })
                return None

            edges = result.get('data', {}).get('products', {}).get('edges', [])
            if not edges:
                print(Fore.RED + f"🚫 No product found for {product_id}" + Style.RESET_ALL)
                missing_products_buffer.append({
                    'product_id': product_id,
                    'supplier_name': id_to_supplier.get(product_id),
                    'reason': "No product found",
                    'http_status': status,
                    'timestamp': datetime.now().isoformat()
                })
                return None

            product = edges[0]['node']
            pid = product['id']

            if pid not in seen_product_ids:
                seen_product_ids.add(pid)
                print(Fore.GREEN + f"✅ Found product {pid}" + Style.RESET_ALL)
                return product
            else:
                return None

    except Exception as e:
        if retries > 0:
            print(Fore.YELLOW + f"🔁 Retrying {product_id} due to error: {e}" + Style.RESET_ALL)
            await asyncio.sleep(2)
            return await fetch_single_product(session, product_id, retries-1)
        else:
            print(Fore.RED + f"🚨 Failed {product_id} after retries: {e}" + Style.RESET_ALL)
            missing_products_buffer.append({
                'product_id': product_id,
                'supplier_name': id_to_supplier.get(product_id),
                'reason': f"Request Failed: {str(e)}",
                'http_status': None,
                'timestamp': datetime.now().isoformat()
            })
            return None

async def worker(queue, session, pbar):
    while not queue.empty():
        product_id = await queue.get()
        product = await fetch_single_product(session, product_id)
        if product:
            all_products_buffer.append(product)

        if len(all_products_buffer) >= 50:
            flush_products_to_csv()
        if len(missing_products_buffer) >= 50:
            flush_missing_to_csv()

        pbar.update(1)
        queue.task_done()

async def main():
    queue = asyncio.Queue()

    for pid in product_ids:
        await queue.put(pid)

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=len(product_ids), desc=Fore.GREEN + "Processing" + Style.RESET_ALL, ncols=120, unit="product", colour='green')
        tasks = []

        for _ in range(concurrent_requests):
            task = asyncio.create_task(worker(queue, session, pbar))
            tasks.append(task)

        await queue.join()

        for task in tasks:
            task.cancel()

        flush_products_to_csv()
        flush_missing_to_csv()
        pbar.close()

if __name__ == "__main__":
    asyncio.run(main())

    # After everything is done
    print(Fore.GREEN + f"\n✅ Total successful products saved: {len(seen_product_ids)}" + Style.RESET_ALL)
    print(Fore.RED + f"🚫 Total missing products saved: {len(missing_products_buffer)}" + Style.RESET_ALL)

    # Also, if some last unsaved data is there (rare cases)
    flush_products_to_csv()
    flush_missing_to_csv()

    print(Fore.CYAN + "\n🎯 All tasks completed!" + Style.RESET_ALL)

