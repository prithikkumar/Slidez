import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import os

# Load supplier IDs from Excel (and ensure uniqueness)
df = pd.read_excel(r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\slidez_final_supplierId.xlsx")
supplier_ids = df['supplier_id'].dropna().astype(str).drop_duplicates().tolist()

# Function to initialize Selenium Chrome WebDriver
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

# Function to save logo to disk
def save_logo(supplier_id, logo_url):
    try:
        response = requests.get(logo_url)
        if response.status_code == 200:
            os.makedirs("logos_13_05_2025", exist_ok=True)
            with open(f"logos_13_05_2025/{supplier_id}_logo.jpg", 'wb') as f:
                f.write(response.content)
            print(f"[✔] Logo saved for {supplier_id}")
        else:
            print(f"[✘] Failed to download logo for {supplier_id} - status code {response.status_code}")
    except Exception as e:
        print(f"[!] Error saving logo for {supplier_id}: {e}")

# Function to scrape logo for a single supplier
def scrape_logo(supplier_id):
    driver = init_driver()
    search_url = f"https://www.google.com/search?q={supplier_id}+logo&tbm=isch"
    try:
        driver.get(search_url)
        sleep(1)  # Small wait to let page load

        image_element = driver.find_element("xpath", "//img[contains(@class, 't0fcAb')]")
        logo_url = image_element.get_attribute("src")

        if logo_url:
            save_logo(supplier_id, logo_url)
            return {'supplier_id': supplier_id, 'status': 'success', 'error_message': '', 'logo_url': logo_url}
        else:
            return {'supplier_id': supplier_id, 'status': 'failed', 'error_message': 'Logo URL not found', 'logo_url': ''}
    except Exception as e:
        return {'supplier_id': supplier_id, 'status': 'failed', 'error_message': str(e), 'logo_url': ''}
    finally:
        driver.quit()

# Parallel logo scraping
def scrape_logos_parallel(supplier_ids, max_workers=20):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scrape_logo, sid) for sid in supplier_ids]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    return results

# Execute scraping
results = scrape_logos_parallel(supplier_ids)

# Create DataFrame from results
output_df = pd.DataFrame(results)

# Summary stats
success_count = (output_df['status'] == 'success').sum()
failed_count = (output_df['status'] == 'failed').sum()
error_count = output_df['error_message'].astype(bool).sum()

# Save to Excel
with pd.ExcelWriter('supplier_logos_results.xlsx') as writer:
    output_df.to_excel(writer, sheet_name='Results', index=False)

    summary_df = pd.DataFrame({
        'Metric': ['Successful scrapes', 'Failed scrapes', 'Error count'],
        'Count': [success_count, failed_count, error_count]
    })
    summary_df.to_excel(writer, sheet_name='Summary', index=False)

# Final summary print
print("\n✅ Scraping complete. Summary:")
print(f"   ✔ Successful: {success_count}")
print(f"   ✘ Failed: {failed_count}")
print(f"   ⚠ Errors: {error_count}")
print("📁 Logos saved to folder: logos_13_05_2025")
print("📄 Results saved to: supplier_logos_results.xlsx")
