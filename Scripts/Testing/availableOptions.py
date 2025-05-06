import pandas as pd
import re
import ast

# Load data
file_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\retry_failed_records.xlsx"
df = pd.read_excel(file_path)

# Fix functions
def fix_invalid_syntax(raw):
    try:
        val = str(raw)
        val = re.sub(r'}\s*{', '}, {', val)
        val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)
        return ast.literal_eval(val)
    except Exception:
        return None

def fix_malformed_node(raw):
    try:
        val = str(raw)
        val = re.sub(r'array\((\[[^\]]*\])\s*,\s*dtype=[^)]+\)', r'\1', val)
        val = re.sub(r'}\s*{', '}, {', val)
        return ast.literal_eval(val)
    except Exception:
        return None

def parse_available_options(raw, error_msg):
    parsed = None
    if 'invalid syntax. Perhaps you forgot a comma?' in str(error_msg):
        parsed = fix_invalid_syntax(raw)
    elif 'malformed node or string on line 1: <ast.Call object' in str(error_msg):
        parsed = fix_malformed_node(raw)
    else:
        return None, "Unsupported error type"

    if parsed is None:
        return None, "Fixing failed"

    for option in parsed:
        if isinstance(option.get('values'), list):
            option['values'] = [str(v) for v in option['values']]
    return parsed, None

# Display samples
def display_samples():
    print("\n=== Sample Fixes for 'invalid syntax' Errors ===")
    syntax_errors = df[df['error'].str.contains("invalid syntax. Perhaps you forgot a comma?", na=False)].head(5)
    for _, row in syntax_errors.iterrows():
        fixed, err = parse_available_options(row['raw'], row['error'])
        print(f"ID: {row['id']}")
        if fixed:
            print(f"Raw Data: {row['raw']}\nFixed Data: {fixed}\n")
        else:
            print(f"Error: {err}\n")

    print("\n=== Sample Fixes for 'malformed node or string' Errors ===")
    malformed_errors = df[df['error'].str.contains("malformed node or string on line 1: <ast.Call object", na=False)].head(5)
    for _, row in malformed_errors.iterrows():
        fixed, err = parse_available_options(row['raw'], row['error'])
        print(f"ID: {row['id']}")
        if fixed:
            print(f"Raw Data: {row['raw']}\nFixed Data: {fixed}\n")
        else:
            print(f"Error: {err}\n")

    print("\n=== Sample Fixes for 'delimiter-related' Errors ===")
    delimiter_candidates = df[
        df['error'].str.contains("invalid syntax", na=False) &
        ~df['error'].str.contains("ast.Call object", na=False)
    ]

    # Further filter to only those that appear to be delimiter issues
    delimiter_errors = delimiter_candidates[delimiter_candidates['raw'].str.contains(r'}\s*{', na=False)].head(5)

    if delimiter_errors.empty:
        print("No delimiter-related records found.")
    for _, row in delimiter_errors.iterrows():
        fixed, err = parse_available_options(row['raw'], row['error'])
        print(f"ID: {row['id']}")
        if fixed:
            print(f"Raw Data: {row['raw']}\nFixed Data: {fixed}\n")
        else:
            print(f"Error: {err}\n")

# Run
if __name__ == "__main__":
    print("Displaying sample fixes based on specific error message...\n")
    display_samples()
