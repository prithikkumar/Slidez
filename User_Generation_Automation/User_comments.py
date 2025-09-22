import random
import datetime
import pytz
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

# US timezone
us_tz = pytz.timezone("US/Eastern")

# ---------------- Comment Generator ---------------- #
def generate_comment_text(product_title, product_desc):
    """Generate short, realistic comment based on product description."""
    templates = [
        f"Love this {product_title}! {product_desc[:50]}…",
        f"Wow, {product_title} looks amazing! {product_desc[:50]}",
        f"Really interested in {product_title}: {product_desc[:50]}",
        f"This seems awesome! {product_title} caught my eye.",
        f"Can you tell more about {product_title}? {product_desc[:40]}",
        f"Great post! {product_title} {product_desc[:50]} impressed me."
    ]
    return random.choice(templates)

# ---------------- Populate Comments ---------------- #
def populate_comments():
    summary_log = []

    # Fetch all users
    users = list(db.collection("users").stream())
    user_docs = {user.id: user.to_dict() for user in users}

    # For each user, check whom they follow
    for user_doc in users:
        user_ref = user_doc.reference
        user_data = user_doc.to_dict()
        username = user_data.get("username")
        if not username:
            print(f"⚠️ User {user_ref.id} has no username, skipping...")
            continue

        following = user_data.get("following", [])
        if not following:
            continue

        # For each followed user
        for followed in following:
            followed_id = followed["id"]
            followed_doc = db.collection("users").document(followed_id).get()
            if not followed_doc.exists:
                continue
            followed_data = followed_doc.to_dict()
            posts_list = followed_data.get("posts", [])
            if not posts_list:
                continue  # Skip if followed user has no posts

            # For each post of the followed user
            for post_id in posts_list:
                post_ref = db.collection("posts").document(post_id)
                post_doc = post_ref.get()
                if not post_doc.exists:
                    continue
                post_data = post_doc.to_dict()
                post_products = post_data.get("products", [])
                if not post_products:
                    continue

                # Randomly decide if this user comments (50% chance)
                if random.random() > 0.5:
                    continue

                for product_id in post_products:
                    product_doc = db.collection("products").document(product_id).get()
                    if not product_doc.exists:
                        continue
                    product_data = product_doc.to_dict()
                    product_title = product_data.get("title", "Product")
                    product_desc = product_data.get("description", "Great product!")

                    comment_text = generate_comment_text(product_title, product_desc)

                    # Check if comment already exists by this user on this post
                    existing_comments = list(
                        post_ref.collection("comments")
                        .where("user_ref", "==", user_ref)
                        .stream()
                    )
                    if existing_comments:
                        continue

                    comment_data = {
                        "comment": comment_text,
                        "user_ref": user_ref,
                        "username": username,
                        "timestamp": datetime.datetime.now(us_tz)
                    }

                    post_ref.collection("comments").add(comment_data)
                    print(f"💬 {username} commented on post {post_id}: {comment_text}")

                    # Update summary
                    summary_log.append({
                        "post_id": post_id,
                        "post_owner": followed_data.get("username", followed_data.get("name", "Unknown")),
                        "commenter": username,
                        "product_id": product_id
                    })

    # ---------------- Summary ---------------- #
    print("\n📊 Summary of Comments Added:")
    summary_count = {}
    for log in summary_log:
        key = log["post_id"]
        summary_count.setdefault(key, {"post_owner": log["post_owner"], "comments": []})
        summary_count[key]["comments"].append({"commenter": log["commenter"], "product_id": log["product_id"]})

    for post_id, data in summary_count.items():
        commenters_products = ", ".join([f"{c['commenter']}->{c['product_id']}" for c in data["comments"]])
        print(f"Post {post_id} by {data['post_owner']}: {len(data['comments'])} comments -> {commenters_products}")

# ---------------- Main ---------------- #
if __name__ == "__main__":
    populate_comments()
