import os
import random
import datetime
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

# Firestore + Storage clients
db = firestore.Client.from_service_account_json(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
storage_client = storage.Client.from_service_account_json(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)

BUCKET_NAME = "slidez-be88c.appspot.com"
bucket = storage_client.bucket(BUCKET_NAME)

# ---------------- Utility Functions ---------------- #

def infer_schema_from_collection(collection_name="users", sample_size=1):
    """Infer schema from a single existing Firestore user doc."""
    docs = db.collection(collection_name).limit(sample_size).stream()
    schema = {}
    for doc in docs:
        data = doc.to_dict()
        for field, value in data.items():
            if field not in schema:
                schema[field] = type(value).__name__
    return schema

def default_value_for_type(field_type):
    """Return default value for missing schema fields."""
    if field_type in ("str", "unicode"): return ""
    if field_type in ("int", "float"): return 0
    if field_type == "bool": return False
    if field_type == "list": return []
    if field_type == "dict": return {}
    return None  # for timestamps or unknown

def get_download_url(blob):
    """Return permanent Firebase public URL."""
    blob_name = blob.name.replace("/", "%2F")
    return f"https://firebasestorage.googleapis.com/v0/b/{BUCKET_NAME}/o/{blob_name}?alt=media"

# ---------------- User Creation ---------------- #

def generate_user_doc(username, blobs, schema):
    """Build Firestore user document from schema + storage files."""
    profile_blob = next((b for b in blobs if "avatar_" in b.name.lower()), blobs[0])
    avatar_url = get_download_url(profile_blob)

    BIO_CHOICES = [
        "Sharing my favorite finds 🛍✨",
        "Lifestyle & fashion 🤝",
        "Foodie 🍔 | Travel ✈ | Marketing tips 💡",
        "Helping Shopify stores scale 🚀",
        "Your daily dose of product reviews 📦",
        "Social media marketer | Growth hacker ⚡",
        "Building community, one post at a time 🤗",
        "Marketing + Creativity = Growth 💡",
        "Unboxing happiness 🎁 | Tag for features 🔖"
    ]

    EMAIL_DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "shopifyconnect.com"]

    doc = {}
    for field, field_type in schema.items():
        if field == "name":
            doc[field] = username.replace("_", " ").split()[0].title()
        elif field == "username":
            doc[field] = username
        elif field == "email":
            domain = random.choice(EMAIL_DOMAINS)
            doc[field] = f"{username}@{domain}"
        elif field in ("avatar", "profile"):
            doc[field] = avatar_url
        elif field == "bio":
            doc[field] = random.choice(BIO_CHOICES)
        elif field.lower().endswith("at"):  # createdAt, updatedAt
            doc[field] = firestore.SERVER_TIMESTAMP
        else:
            doc[field] = default_value_for_type(field_type)
    return doc

def bulk_create_users_from_storage(prefix="user_profiles/"):
    """Scan Firebase Storage and create users if they don’t exist in Firestore."""
    schema = infer_schema_from_collection("users")
    print("📋 Inferred schema:", schema)

    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=prefix))
    user_files = {}
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) < 2 or not parts[1]:
            continue
        username = parts[1]
        user_files.setdefault(username, []).append(blob)

    for username, files in user_files.items():
        # Check if username already exists
        existing_users = db.collection("users").where("username", "==", username).limit(1).stream()
        if any(existing_users):
            print(f"⚠️ User already exists: {username}, skipping...")
            continue

        user_doc = generate_user_doc(username, files, schema)
        # Let Firestore auto-generate document ID
        doc_ref = db.collection("users").document()
        doc_ref.set(user_doc)
        print(f"✅ Created user: {username} → Document ID: {doc_ref.id}")

# ---------------- Main ---------------- #

if __name__ == "__main__":
    bulk_create_users_from_storage()
