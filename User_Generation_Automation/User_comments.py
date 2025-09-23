import os
import random
import datetime
import pytz
import firebase_admin
from firebase_admin import credentials, firestore

# ========== Firebase Setup ==========
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

# US timezone
us_tz = pytz.timezone("US/Eastern")

# ========== Generic Fallback Comments ==========
GENERIC_COMMENTS = [
    "Looks amazing 🔥",
    "Wow, love this!",
    "This is so cool 😍",
    "Great post 👍",
    "Awesome product!",
    "I need this in my life 😮",
    "Beautiful shot 📸",
    "Absolutely stunning!",
]

# ========== Helper Functions ==========
def generate_product_comment(product_desc: str) -> str:
    """Generate a comment inspired by product description."""
    if not product_desc:
        return random.choice(GENERIC_COMMENTS)

    keywords = product_desc.split()[:10]  # first 10 words
    base = " ".join(keywords)
    options = [
        f"This {base} looks amazing!",
        f"Love how this {base} is described 👌",
        f"Wow, I really like this {base} 😍",
        f"This product seems perfect: {base}",
    ]
    return random.choice(options)

def add_comments_to_post(post_ref, post_owner_id, target_users, summary_log, product_desc=None):
    """Add up to 3 comments from other target users and update summary log."""
    commenters = [u for u in target_users if u.id != post_owner_id]
    if not commenters:
        return

    num_comments = random.randint(1, min(3, len(commenters)))  # max 3 comments
    selected_commenters = random.sample(commenters, num_comments)

    for commenter in selected_commenters:
        commenter_data = commenter.to_dict()
        commenter_name = commenter_data.get("username", "anon")

        if product_desc:  # product-based comment
            comment_text = generate_product_comment(product_desc)
        else:  # fallback generic comment
            comment_text = random.choice(GENERIC_COMMENTS)

        comment_data = {
            "user_ref": commenter.reference,
            "username": commenter_name,
            "comment": comment_text,
            "timestamp": datetime.datetime.now(us_tz),
        }

        post_ref.collection("comments").add(comment_data)
        print(f"💬 {commenter_name} commented on post {post_ref.id}: {comment_text}")

        # Update summary log
        summary_log.append({
            "post_id": post_ref.id,
            "post_owner": post_owner_id,
            "commenter": commenter_name,
            "comment": comment_text
        })

# ========== Main Script ==========
def populate_comments(folder_path):
    summary_log = []

    # 1. Collect target usernames from folder names
    target_usernames = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    print(f"📂 Found {len(target_usernames)} target users from folders.")

    # 2. Map usernames to Firestore users
    target_users = []
    for username in target_usernames:
        query = db.collection("users").where("username", "==", username).stream()
        docs = list(query)
        if docs:
            target_users.append(docs[0])
        else:
            print(f"⚠️ No user found for folder {username}")

    print(f"✅ Matched {len(target_users)} users in Firestore.")

    # 3. Loop through each user’s posts
    for user_doc in target_users:
        user_id = user_doc.id
        user_data = user_doc.to_dict()
        username = user_data.get("username", "anon")

        posts = db.collection("posts").where("user_ref", "==", user_doc.reference).stream()
        post_list = list(posts)
        if not post_list:
            continue

        for post in post_list:
            post_ref = post.reference
            post_data = post.to_dict()

            # Determine product description if available
            product_desc = None
            if "products" in post_data and post_data["products"]:
                product_id = post_data["products"][0]  # take first product
                product_doc = db.collection("products").document(product_id).get()
                if product_doc.exists:
                    product_data = product_doc.to_dict()
                    product_desc = product_data.get("description")

            add_comments_to_post(post_ref, user_id, target_users, summary_log, product_desc)

    # ========== Summary ==========
    print("\n📊 Summary of Comments Added:")
    summary_count = {}
    for log in summary_log:
        key = log["post_id"]
        summary_count.setdefault(key, {"post_owner": log["post_owner"], "comments": []})
        summary_count[key]["comments"].append({
            "commenter": log["commenter"],
            "comment": log["comment"]
        })

    for post_id, data in summary_count.items():
        comment_texts = ", ".join([f"{c['commenter']}: \"{c['comment']}\"" for c in data["comments"]])
        print(f"Post {post_id} by {data['post_owner']}: {len(data['comments'])} comments -> {comment_texts}")

# ========== Run ==========
if __name__ == "__main__":
    folder_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles"  # <-- update to your folder path
    populate_comments(folder_path)