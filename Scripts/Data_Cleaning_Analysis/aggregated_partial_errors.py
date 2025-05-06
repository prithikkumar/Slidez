import pandas as pd
import re

# Load the failed records Excel
df = pd.read_excel("retry_failed_records.xlsx")

# Normalize error messages
def normalize_error_message(msg):
    if pd.isna(msg):
        return "Unknown Error"
    msg = str(msg)
    msg = re.sub(r"<.*?object at 0x[0-9A-Fa-f]+>", "<object>", msg)
    return msg.strip()

# Apply normalization
df["normalized_error"] = df["error"].apply(normalize_error_message)

# Group and count by normalized error message
error_summary = df["normalized_error"].value_counts().reset_index()
error_summary.columns = ["Error Message", "Count"]

# Display the summary
print("\n=== Aggregated Error Summary ===")
for idx, row in error_summary.iterrows():
    print(f"[{row['Count']}] {row['Error Message']}")
