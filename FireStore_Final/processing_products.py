import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from tqdm import tqdm
from datetime import datetime, UTC
from typing import List, Dict, Any, Optional

INPUT_FILE = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Processed_Input\Products_Final\products.jsonl'  # Your input
OUTPUT_FILE = r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\products_final.jsonl'
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ===== Helper Functions =====

def normalize_updatedAt(updated_at):
    try:
        if isinstance(updated_at, (int, float)):
            return datetime.fromtimestamp(updated_at / 1000, UTC).isoformat()
        elif isinstance(updated_at, str) and updated_at.isdigit():
            return datetime.fromtimestamp(int(updated_at) / 1000, UTC).isoformat()
    except Exception as e:
        return None

# def sanitize_description(description: Optional[str], description_html: Optional[str]) -> Optional[str]:
#     if (not description or description.strip() == "") and description_html:
#         return re.sub(r"<[^>]*>", " ", description_html).strip()
#     return description



def fix_images_array(images: Optional[List[Dict[str, Any]]], featured_image: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Step 1: Fallback to featuredImage if images is empty or None
    if not images and featured_image and featured_image.get("mediaURL"):
        return [{
            "altText": featured_image.get("altText", ""),
            "id": featured_image.get("id", ""),
            "mediaURL": featured_image["mediaURL"],
            "position": 0
        }]

    images = images or []

    # Step 2: Deduplicate by `position` (keep first occurrence)
    seen_positions = set()
    deduped_images = []
    for img in images:
        pos = img.get("position")
        if pos not in seen_positions:
            seen_positions.add(pos)
            deduped_images.append(img)

    return deduped_images


def cast_total_variants(value: Any) -> int:
    try:
        return int(value)
    except:
        return 0

# ===== For Sample Output Comparison =====
preview_limit = 3
preview_samples = []

# ===== Process Records =====
processed_total = 0
chunk_size = 1000
output_lines = []

with open(INPUT_FILE, "r", encoding="utf-8") as infile:
    for line in tqdm(infile, desc="🔄 Processing Records"):
        try:
            raw = json.loads(line.strip())
            processed = raw.copy()

            # Original values for preview
            preview_record = {}

            record_id = raw.get("id", f"record_{processed_total}")
            preview_record = {"id": record_id}

            preview_record["updatedAt_before"] = raw.get("updatedAt")
            processed["updatedAt"] = normalize_updatedAt(raw.get("updatedAt"))
            preview_record["updatedAt_after"] = processed["updatedAt"]

            # preview_record["description_before"] = raw.get("description")
            # processed["description"] = sanitize_description(raw.get("description"), raw.get("descriptionHtml"))
            # preview_record["description_after"] = processed["description"]

            preview_record["images_before"] = raw.get("images")
            processed["images"] = fix_images_array(raw.get("images"), raw.get("featuredImage"))
            preview_record["images_after"] = processed["images"]

            preview_record["totalVariants_before"] = raw.get("totalVariants")
            processed["totalVariants"] = cast_total_variants(raw.get("totalVariants"))
            preview_record["totalVariants_after"] = processed["totalVariants"]

            if processed_total < preview_limit:
                preview_samples.append(preview_record)

            output_lines.append(json.dumps(processed, ensure_ascii=False))
            processed_total += 1

        except Exception as e:
            print(f"❌ Error: {e}")

# ===== Write Output =====
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(output_lines))

# ===== Show Sample Changes =====
print("\n🧪 Preview of Field Transformations:")
for i, sample in enumerate(preview_samples):
    print(f"\n--- Record {i+1} (ID: {sample.get('id', 'N/A')}) ---")
    for key, value in sample.items():
        if key != "id":
            print(f"{key}: {value}")

# ===== Print Schema (like printSchema) =====
def infer_schema_from_sample(record: Dict[str, Any]) -> Dict[str, str]:
    def type_of(v):
        if isinstance(v, str):
            return "StringType"
        elif isinstance(v, bool):
            return "BooleanType"
        elif isinstance(v, int):
            return "IntegerType"
        elif isinstance(v, float):
            return "DoubleType"
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                return f"ArrayType(StructType(...))"
            return "ArrayType(StringType)"
        elif isinstance(v, dict):
            return "StructType(...)"
        elif v is None:
            return "NullType"
        return "UnknownType"
    return {k: type_of(v) for k, v in record.items()}

sample_record = json.loads(output_lines[0])
schema = infer_schema_from_sample(sample_record)

print("\n📘 Output Schema (like Spark DataFrame):")
for key, dtype in schema.items():
    print(f"|-- {key}: {dtype}")

print(f"\n✅ Finished! Processed total: {processed_total} records.")
print(f"📁 Saved to: {OUTPUT_FILE}")
