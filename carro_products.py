import pandas as pd
import asyncio
import aiohttp
import os
import time
from datetime import datetime
from tqdm import tqdm
from colorama import Fore, Style

# Load product IDs
df_ids = pd.read_csv("Raw_Data\Slidez_products_excel.csv")
product_ids = df_ids['retailer_product_id'].dropna().astype(str).unique().tolist()
print(f"📦 Total product IDs: {len(product_ids)}")

graphql_url = 'https://api.getcarro.com/graphql'
graphql_query = """query ($productIds: [ID!], $first: Int!) {
  products(productIds: $productIds, first: $first) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        title
        descriptionHtml
        totalVariants
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
    'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im5JNVZmby1SVG5nMWNNUm9uZ0t2VSJ9.eyJtb2duZXQvYXNzb2NpYXRlZF9icmFuZF9pZCI6Im1reDBsMWprejIxdHNzNTRobnE1NHo0ZSIsImlzcyI6Imh0dHBzOi8vdnlybC51cy5hdXRoMC5jb20vIiwic3ViIjoiNHJVbFR5MkJVYnNXNTAxOVdRVW43Q3hyZDlLZUR1SDBAY2xpZW50cyIsImF1ZCI6InVybjpjYXJyby1wbGF0Zm9ybSIsImlhdCI6MTc0NDU3NjAwMywiZXhwIjoxNzQ3MTY4MDAzLCJzY29wZSI6InJlYWQ6ZGlyZWN0b3J5IHJlYWQ6cHJvZHVjdHMgd3JpdGU6cHJvZHVjdHMgcmVhZDpkb2NzIGNyZWF0ZTp1c2VycyByZWFkOm9yZGVycyB3cml0ZTpvcmRlcnMiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiI0clVsVHkyQlVic1c1MDE5V1FVbjdDeHJkOUtlRHVIMCIsInBlcm1pc3Npb25zIjpbInJlYWQ6ZGlyZWN0b3J5IiwicmVhZDpwcm9kdWN0cyIsIndyaXRlOnByb2R1Y3RzIiwicmVhZDpkb2NzIiwiY3JlYXRlOnVzZXJzIiwicmVhZDpvcmRlcnMiLCJ3cml0ZTpvcmRlcnMiXX0.bhCKskZBNKbpPP4uqIYIaWuk305OHg9TvUWLFXEIk6SvSYdvodiklwiVE_u27bo_B1McvzmEL-JC_dZvHdbQVYpfM_HEkyhNzzapBg1cpCYuOEBOjM7rnSyMBCclhgzrvoGjkWpfqZODBUSXHIy1GuknILVp-BOdAJwzs0gTOEbFqAK-QrcvVv7xDOjmp1LzS58fNyS7HBWF2wS09gsTo1TQpQ5bVEnkyIpe79g4XmvXvNCIuW0fnGjn4mVBCZ890gmeA8u-bMtK7Zx0p_B3Vzt9t3gXhpTFwKdoheaqL9XaBHRWjU_Ns5NDKPFTxRZAJupJhxKG3tivay5DxLHJVQ'
}

output_file = "Raw_Data/filtered_carro_products_final.csv"
batch_size = 25
concurrent_batches = 3

all_products_buffer = []
seen_product_ids = set()
failed_batches = []
last_write_time = time.time()
write_interval = 30

def flush_to_csv():
    if all_products_buffer:
        df_temp = pd.DataFrame(all_products_buffer)
        write_mode = 'a' if os.path.exists(output_file) else 'w'
        header = not os.path.exists(output_file)
        df_temp.to_csv(output_file, mode=write_mode, index=False, header=header)
        print(Fore.GREEN + f"💾 Flushed {len(df_temp)} records at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + Style.RESET_ALL)
        all_products_buffer.clear()

async def fetch_batch(session, batch, start_idx, retries=1):
    try:
        async with session.post(graphql_url, headers=headers, json={
            'query': graphql_query,
            'variables': {
                'productIds': batch,
                'first': len(batch)
            }
        }, timeout=aiohttp.ClientTimeout(total=15)) as response:
            result = await response.json()
            if 'errors' in result:
                print(Fore.RED + f"❌ GraphQL error on batch starting at {start_idx}: {result['errors']}" + Style.RESET_ALL)
                return []
            edges = result.get('data', {}).get('products', {}).get('edges', [])
            products = []
            for edge in edges:
                product = edge['node']
                product_id = product['id']
                if product_id not in seen_product_ids:
                    products.append(product)
                    seen_product_ids.add(product_id)
            print(Fore.GREEN + f"✅ Batch {start_idx}-{start_idx+len(batch)-1} fetched {len(products)} new products" + Style.RESET_ALL)
            return products
    except Exception as e:
        if retries > 0:
            print(Fore.YELLOW + f"🔁 Retrying batch starting at {start_idx} due to error: {e}" + Style.RESET_ALL)
            await asyncio.sleep(2)
            return await fetch_batch(session, batch, start_idx, retries=retries-1)
        else:
            print(Fore.RED + f"🚨 Failed batch starting at {start_idx} after retries: {e}" + Style.RESET_ALL)
            failed_batches.append(batch)
            return []

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        total_batches = len(product_ids) // batch_size + int(len(product_ids) % batch_size != 0)
        
        # Green Progress Bar with speed and ETA
        pbar = tqdm(total=total_batches, desc=Fore.GREEN + "Processing" + Style.RESET_ALL, 
                    ncols=120, unit="batch", colour='green', dynamic_ncols=True,
                    smoothing=0.2)  # smoothing controls ETA stability
        
        batches_processed = 0
        
        start_time = time.time()

        for i in range(0, len(product_ids), batch_size):
            batch = product_ids[i:i + batch_size]
            tasks.append(fetch_batch(session, batch, i))

            if len(tasks) >= concurrent_batches:
                results = await asyncio.gather(*tasks)
                for batch_products in results:
                    all_products_buffer.extend(batch_products)
                tasks = []

                flush_to_csv()  # flush first
                pbar.update(len(results))  # then update bar after successful save
                batches_processed += len(results)

                elapsed = time.time() - start_time
                speed = batches_processed / elapsed
                eta = (total_batches - batches_processed) / speed if speed > 0 else 0
                pbar.set_postfix({
                    'Speed': f"{speed:.2f} batch/s",
                    'ETA': f"{eta/60:.1f} min"
                })

                await asyncio.sleep(1)

        # Finish leftover tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            for batch_products in results:
                all_products_buffer.extend(batch_products)
            flush_to_csv()
            pbar.update(len(results))
            batches_processed += len(results)

            elapsed = time.time() - start_time
            speed = batches_processed / elapsed
            eta = (total_batches - batches_processed) / speed if speed > 0 else 0
            pbar.set_postfix({
                'Speed': f"{speed:.2f} batch/s",
                'ETA': f"{eta/60:.1f} min"
            })

        pbar.close()

        if failed_batches:
            print(Fore.YELLOW + f"❗ Retrying {len(failed_batches)} failed batches..." + Style.RESET_ALL)
            for i, failed_batch in enumerate(failed_batches):
                retry_results = await fetch_batch(session, failed_batch, i)
                if retry_results:
                    all_products_buffer.extend(retry_results)
            flush_to_csv()

if __name__ == "__main__":
    asyncio.run(main())
    print(Fore.GREEN + "✅ All done! Data saved to filtered_carro_products.csv" + Style.RESET_ALL)