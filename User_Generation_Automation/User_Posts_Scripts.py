import os
import random
import re
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage

# 🔑 Initialize Firebase
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)

firebase_admin.initialize_app(cred, {
    "storageBucket": "slidez-be88c.appspot.com"
})

# Clients
db = firestore.Client.from_service_account_json(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
storage_client = storage.Client.from_service_account_json(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)

BUCKET_NAME = "slidez-be88c.appspot.com"
bucket = storage_client.bucket(BUCKET_NAME)

# ------------------ Helpers ------------------

def infer_schema_from_collection(collection_name="posts", sample_size=5):
    """Infer schema dynamically from Firestore collection (sample docs)."""
    docs = db.collection(collection_name).limit(sample_size).stream()
    schema = {}
    for doc in docs:
        for field, value in doc.to_dict().items():
            if field not in schema:
                schema[field] = type(value).__name__
    return schema

def get_download_url(blob):
    """Return permanent Firebase public URL."""
    blob_name = blob.name.replace("/", "%2F")
    return f"https://firebasestorage.googleapis.com/v0/b/{BUCKET_NAME}/o/{blob_name}?alt=media"

def generate_dynamic_caption(product_name: str, product_desc: str) -> str:
    """Generate a short social-media style caption using product description."""
    clean_desc = re.sub(r"[^a-zA-Z0-9\s]", "", product_desc).strip()
    words = clean_desc.split()
    keywords = " ".join(words[:12]) + ("..." if len(words) > 12 else "")
    
    templates = [
        "Obsessed with {product} — {keywords}! {emoji}",
        "Latest drop: {product}. {keywords} {emoji}",
        "When you need {keywords}, {product} has your back {emoji}",
        "Vibes ✨ {product}: {keywords}",
        "{emoji} {product} → {keywords}",
        "Weekend calls for {product}. {keywords} {emoji}",
    ]
    emojis = ["😍", "🔥", "👌", "💯", "⚡", "✨", "🖤", "🎉"]
    
    template = random.choice(templates)
    return template.format(
        product=product_name.capitalize(),
        keywords=keywords,
        emoji=random.choice(emojis)
    )

def fetch_product_info_from_filename(filename: str):
    """Extract product info from filename prefix (before _) and fetch Firestore product doc."""
    product_prefix = filename.split("_")[0]
    product_docs = db.collection("products").where("title", "==", product_prefix).limit(1).stream()
    for doc in product_docs:
        return doc.id, doc.to_dict()
    return None, None

def create_post_doc(username: str, blob, schema):
    """Build a Firestore post doc based on inferred schema and product mapping."""
    product_id, product_data = fetch_product_info_from_filename(os.path.basename(blob.name))
    if not product_id or not product_data:
        print(f"⚠️ No product found for file {blob.name}")
        return None

    # Caption from product description
    caption = generate_dynamic_caption(product_data.get("title", ""), product_data.get("description", ""))

    # Get user reference (document ID by username)
    user_query = db.collection("users").where("username", "==", username).limit(1).stream()
    user_ref = None
    for u in user_query:
        user_ref = db.collection("users").document(u.id)
        break
    if not user_ref:
        print(f"⚠️ User not found for {username}")
        return None

    # Media type
    ext = blob.name.split(".")[-1].lower()
    media_type = "video" if ext in ["mp4", "mov", "avi"] else "image"

    # Prevent duplicate posts
    existing = db.collection("posts").where("content.Media.url", "==", get_download_url(blob)).limit(1).stream()
    if any(existing):
        print(f"⏩ Skipped duplicate post for file {blob.name}")
        return None

    # Construct doc
    post_doc = {}
    for field, field_type in schema.items():
        if field == "user_ref":
            post_doc[field] = user_ref
        elif field == "content":
            post_doc[field] = {
                "caption": caption,
                "Media": {
                    "type": media_type,
                    "url": get_download_url(blob)
                },
                "Products": [product_id]
            }
        else:
            post_doc[field] = None
    return post_doc

# ------------------ Main ------------------

def generate_posts_from_storage(prefix="user_profiles/"):
    schema = infer_schema_from_collection("posts")
    print("📋 Inferred schema:", schema)

    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=prefix))
    user_files = {}
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) < 2 or not parts[1]:
            continue
        username = parts[1]
        if "avatar_" not in blob.name:  # exclude avatars
            user_files.setdefault(username, []).append(blob)

    for username, files in user_files.items():
        for blob in files:
            post_doc = create_post_doc(username, blob, schema)
            if post_doc:
                db.collection("posts").add(post_doc)
                print(f"✅ Created post for user {username}: {blob.name}")

if __name__ == "__main__":
    generate_posts_from_storage()