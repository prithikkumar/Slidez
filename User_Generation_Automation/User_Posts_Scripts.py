import os
import random
import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage

# ---------------- CONFIG ----------------
SERVICE_ACCOUNT = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
BUCKET_NAME = "slidez-be88c.appspot.com"
STORAGE_PREFIX = "user_profiles/"  # prefix where user folders live in storage
# ----------------------------------------

# Initialize Firebase + clients
cred = credentials.Certificate(SERVICE_ACCOUNT)
firebase_admin.initialize_app(cred, {"storageBucket": BUCKET_NAME})

db = firestore.Client.from_service_account_json(SERVICE_ACCOUNT)
storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT)
bucket = storage_client.bucket(BUCKET_NAME)


# ---------------- Utilities ----------------
def get_download_url(blob):
    name = blob.name.replace("/", "%2F")
    return f"https://firebasestorage.googleapis.com/v0/b/{BUCKET_NAME}/o/{name}?alt=media"


def infer_sample_post_doc():
    """Return the first post doc as a dict (used to detect exact field names/casing)."""
    docs = db.collection("posts").limit(1).stream()
    doc = next(docs, None)
    if doc:
        return doc.to_dict()
    return None


def find_field_name_case_insensitive(keys, target):
    """Return the actual key from keys whose lowercase equals target.lower(), else None."""
    target_l = target.lower()
    for k in keys:
        if k.lower() == target_l:
            return k
    return None


def try_product_lookup(name):
    """Try several variants to find a product doc by title. Returns DocumentSnapshot or None."""
    tests = [name, name.strip(), name.replace("_", " "), name.replace("-", " "), name.title(), name.capitalize()]
    seen = set()
    for t in tests:
        tv = t.strip()
        if not tv or tv in seen:
            continue
        seen.add(tv)
        q = db.collection("products").where("title", "==", tv).limit(1).stream()
        doc = next(q, None)
        if doc:
            return doc
    return None


def post_exists_for_user_media(user_ref, media_url):
    """Check if a post exists for this user and media URL (handle Media/media casing)."""
    posts = db.collection("posts")
    q1 = posts.where("user_ref", "==", user_ref).where("content.Media.url", "==", media_url).limit(1).stream()
    if any(q1):
        return True
    q2 = posts.where("user_ref", "==", user_ref).where("content.media.url", "==", media_url).limit(1).stream()
    if any(q2):
        return True
    # also try 'user' field name in case schema uses different key
    q3 = posts.where("user", "==", user_ref).where("content.Media.url", "==", media_url).limit(1).stream()
    if any(q3):
        return True
    q4 = posts.where("user", "==", user_ref).where("content.media.url", "==", media_url).limit(1).stream()
    if any(q4):
        return True
    return False


def generate_caption_from_products(titles, descriptions):
    """Create a realistic, slightly random caption using titles and descriptions."""
    # If multiple products, join nicely
    if len(titles) > 1:
        product_part = " + ".join(titles)
    else:
        product_part = titles[0]

    # pick up to two non-empty description snippets
    snippets = [d.strip() for d in descriptions if d and d.strip()]
    snippet = ""
    if snippets:
        # choose random snippet and truncate to ~80 chars
        chosen = random.choice(snippets)
        snippet = chosen[:120].rstrip()
        if len(chosen) > len(snippet):
            snippet += "..."
    # templates
    templates = [
        "{product} — {snippet} {emoji}",
        "New drop: {product}! {snippet} {emoji}",
        "Loving {product} lately {emoji} {snippet}",
        "{emoji} {product} — {snippet}",
        "{product}: {snippet}"
    ]
    emoji = random.choice(["😍", "🔥", "✨", "⚡", "👌", "🖤"])
    template = random.choice(templates)
    caption = template.format(product=product_part, snippet=(snippet or "Check it out"), emoji=emoji)
    return caption


# ---------------- Post Document Builder ----------------
def build_post_doc(user_ref, blob, sample_doc):
    """
    Build a post document dict using a sample_doc to preserve exact field names/casing.
    Returns tuple (post_doc_dict, failure_reason_or_None)
    """
    filename = os.path.basename(blob.name)
    if filename.lower().startswith("avatar_"):
        return None, "avatar_file"

    # product names: filename without extension, split on &&
    base = os.path.splitext(filename)[0]
    product_names = [p.strip() for p in base.split("&&") if p.strip()]
    if not product_names:
        return None, "no_product_prefix"

    # lookup products, collect ids, titles, descriptions
    product_ids = []
    titles = []
    descriptions = []
    for pn in product_names:
        pdoc = try_product_lookup(pn)
        if pdoc:
            product_ids.append(pdoc.id)
            pdata = pdoc.to_dict()
            titles.append(pdata.get("title", pn))
            descriptions.append(pdata.get("description", ""))
        else:
            # If any product not found -> skip creation (user wanted strict matches)
            return None, f"product_not_found:{pn}"

    # Fetch media url and type
    media_url = get_download_url(blob)
    ext = filename.lower().split(".")[-1]
    media_type = "video" if ext in ("mp4", "mov", "avi") else "image"

    # Prevent duplicates per user
    if post_exists_for_user_media(user_ref, media_url):
        return None, "duplicate"

    # Compose caption
    caption = generate_caption_from_products(titles, descriptions)

    # Build doc using sample_doc structure if present, else default keys
    post_doc = {}

    if sample_doc:
        # top-level field names
        top_keys = list(sample_doc.keys())
        # find 'content' field name
        content_key = find_field_name_case_insensitive(top_keys, "content") or "content"
        # find user field name
        user_key = find_field_name_case_insensitive(top_keys, "user_ref") or find_field_name_case_insensitive(top_keys, "user") or "user_ref"

        # create copy of sample structure with defaults
        # For simplicity we'll only fill top-level keys required and defaults for others
        for k in top_keys:
            if k == user_key:
                post_doc[k] = user_ref
            elif k == content_key:
                # decide inner keys' casing based on sample if possible
                inner = {}
                sample_inner = sample_doc.get(content_key, {})
                # find media key name in sample inner
                media_key = find_field_name_case_insensitive(list(sample_inner.keys()), "media") or "media"
                products_key = find_field_name_case_insensitive(list(sample_inner.keys()), "products") or "products"
                caption_key = find_field_name_case_insensitive(list(sample_inner.keys()), "caption") or "caption"

                inner[caption_key] = caption
                inner[media_key] = {"type": media_type, "url": media_url}
                inner[products_key] = product_ids
                post_doc[k] = inner
            elif k.lower().endswith("at"):
                post_doc[k] = firestore.SERVER_TIMESTAMP
            else:
                # fill simple default by type
                val = sample_doc.get(k, None)
                if isinstance(val, str):
                    post_doc[k] = ""
                elif isinstance(val, bool):
                    post_doc[k] = False
                elif isinstance(val, (int, float)):
                    post_doc[k] = 0
                elif isinstance(val, list):
                    post_doc[k] = []
                elif isinstance(val, dict):
                    post_doc[k] = {}
                else:
                    post_doc[k] = None
    else:
        # No sample doc: create default minimal schema
        post_doc = {
            "user_ref": user_ref,
            "content": {
                "caption": caption,
                "media": {"type": media_type, "url": media_url},
                "products": product_ids
            },
            "createdAt": firestore.SERVER_TIMESTAMP
        }

    return post_doc, None


# ---------------- Main bulk creation ----------------
def bulk_create_posts_from_storage(prefix=STORAGE_PREFIX):
    sample_doc = infer_sample_post_doc()  # may be None
    if sample_doc:
        print("📋 Sample post doc keys (used to respect schema casing):", list(sample_doc.keys()))
    else:
        print("⚠️ No posts found in collection — using default schema keys.")

    # list all blobs under prefix and group by username (folder)
    blobs = list(bucket.list_blobs(prefix=prefix))
    user_files = {}
    for b in blobs:
        parts = b.name.split("/")
        # expected: user_profiles/<username>/<filename>
        if len(parts) < 3:
            continue
        username = parts[1]
        user_files.setdefault(username, []).append(b)

    total_created = 0
    total_skipped = 0

    # Per-user processing
    for username, blobs in user_files.items():
        # find user document by username field
        user_query = db.collection("users").where("username", "==", username).limit(1).stream()
        user_snapshot = next(user_query, None)
        if not user_snapshot:
            print(f"\n⚠️ User not found in users collection for username '{username}' — skipping entire folder.")
            continue
        user_ref = user_snapshot.reference

        # created_ids = []
        created_products = []
        processed_files = []
        actual_media_count = 0

        for blob in blobs:
            filename = os.path.basename(blob.name)
            if filename.lower().startswith("avatar_"):
                # skip avatar files (do not count as media)
                continue

            actual_media_count += 1

            post_doc, reason = build_post_doc(user_ref, blob, sample_doc)
            if post_doc is None:
                total_skipped += 1
                # log reason
                print(f"⏩ Skipped '{filename}' for '{username}' (reason: {reason})")
                continue

            # create post with auto-generated doc id
            new_ref = db.collection("posts").document()
            new_ref.set(post_doc)

            # pull product ids from content
            content = post_doc.get("content") or post_doc.get("Content") or {}
            products = content.get("products") or content.get("Products") or []
            created_products.extend(products)

            # created_ids.append(new_ref.id)
            processed_files.append(filename)
            total_created += 1

            # log creation details
            # find content url key robustly
            content = post_doc.get("content") or post_doc.get("Content") or {}
            media = content.get("media") or content.get("Media") or {}
            url = media.get("url")
            print(f"✅ Created post {new_ref.id} for user '{username}': file='{filename}', url='{url}', products={content.get('products') or content.get('Products')}")

        # per-user summary
        print(f"\n📌 Summary for user: {username}")
        print(f"   Actual media files in folder: {actual_media_count}")
        # print(f"   Posts created (doc ids): {created_ids}")
        print(f"   Products referenced in created posts: {created_products}")
        print(f"   Media files processed: {processed_files}")

    # global summary
    print(f"\n🚀 Finished. Total posts created: {total_created}, total skipped: {total_skipped}")


if __name__ == "__main__":
    bulk_create_posts_from_storage()
