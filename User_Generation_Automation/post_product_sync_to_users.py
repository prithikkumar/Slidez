import firebase_admin
from firebase_admin import credentials, firestore

# 🔑 Initialize Firebase
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

def sync_posts_to_users():
    posts_ref = db.collection("posts")
    posts = posts_ref.stream()

    updated_users = {}

    # 📊 Counters
    total_posts = 0
    new_posts_added = 0
    posts_already_synced = 0
    total_products_added = 0

    for post in posts:
        total_posts += 1
        post_id = post.id
        post_data = post.to_dict()

        user_ref = post_data.get("user_ref")
        products = post_data.get("content", {}).get("products", [])

        if not user_ref:
            print(f"⚠️ Post {post_id} missing user_ref, skipping...")
            continue

        user_id = user_ref.id
        user_doc_ref = db.collection("users").document(user_id)
        user_doc = user_doc_ref.get()

        if not user_doc.exists:
            print(f"⚠️ User {user_id} not found for post {post_id}, skipping...")
            continue

        user_data = user_doc.to_dict()
        existing_posts = user_data.get("posts", [])
        existing_products = user_data.get("products", [])

        updates = {}

        if post_id not in existing_posts:
            # New post → add post and products
            updates["posts"] = firestore.ArrayUnion([post_id])
            if products:
                updates["products"] = firestore.ArrayUnion(products)
            new_posts_added += 1
            total_products_added += len(products)
            log_msg = f"🆕 Added new post {post_id} and products {products} to user {user_id}"
        else:
            # Post already exists → check products
            missing_products = [p for p in products if p not in existing_products]
            if missing_products:
                updates["products"] = firestore.ArrayUnion(missing_products)
                total_products_added += len(missing_products)
                log_msg = f"➕ Post {post_id} already exists, added missing products {missing_products} to user {user_id}"
            else:
                posts_already_synced += 1
                log_msg = f"✔️ Post {post_id} already synced for user {user_id}, no changes"

        if updates:
            user_doc_ref.update(updates)

            updated_users.setdefault(user_id, {"posts": [], "products": []})
            if "posts" in updates:
                updated_users[user_id]["posts"].append(post_id)
            if "products" in updates:
                updated_users[user_id]["products"].extend(updates["products"].values)

        print(log_msg)

    # 📊 Summary log
    print("\n📊 Sync Summary:")
    print(f"   📝 Total posts scanned: {total_posts}")
    print(f"   🆕 New posts added: {new_posts_added}")
    print(f"   ✔️ Posts already synced (no changes): {posts_already_synced}")
    print(f"   📦 Total products added: {total_products_added}\n")

    for user_id, data in updated_users.items():
        print(f"👤 User: {user_id}")
        if data["posts"]:
            print(f"   📝 Posts added: {data['posts']}")
        if data["products"]:
            print(f"   📦 Products added: {list(set(data['products']))}")

if __name__ == "__main__":
    sync_posts_to_users()
    print("\n✅ Incremental sync completed.")
