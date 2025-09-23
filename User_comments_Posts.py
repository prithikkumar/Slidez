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

# ========== Generic Social Media Comments ==========
GENERIC_COMMENTS = [
    "🔥 Loving this campaign!", 
    "💡 Great strategy here!", 
    "👏 Nice engagement idea!", 
    "📈 This is really effective!", 
    "😍 Amazing content!", 
    "👍 Definitely shareable!", 
    "💯 Impressive reach potential!", 
    "💡 Clever approach!", 
    "🎯 Perfect targeting!", 
    "💬 This would spark great conversation!", 
    "📸 Really eye-catching visuals!", 
    "✨ Creative post!", 
    "🙌 Love this concept!", 
    "🎉 Exciting idea!", 
    "💥 Strong messaging!", 
    "😎 Such a vibe for social media!", 
    "💫 Engaging and trendy!", 
    "❤️ This would get a lot of likes!", 
    "📣 Very shareable content!", 
    "👏 Nicely executed!"
]

EMOJIS = ["🔥","😍","😎","👍","💯","😮","📸","✨","🤩","😲","😃","❤️","👏","🎉","💡","🙌","🎯","💥","💫"]

# ========== Helper Function ==========
def add_comments_to_post(post_ref, post_owner_id, target_users, summary_log):
    """Add generic comments from users who haven't commented yet; max 2 identical per post."""
    existing_comments_docs = post_ref.collection("comments").stream()
    existing_commenters = {doc.to_dict().get("username") for doc in existing_comments_docs}
    comment_text_count = {}
    for doc in existing_comments_docs:
        text = doc.to_dict().get("comment")
        if text:
            comment_text_count[text] = comment_text_count.get(text, 0) + 1

    for commenter in target_users:
        commenter_data = commenter.to_dict()
        commenter_name = commenter_data.get("username", "anon")

        if commenter_name in existing_commenters or commenter.id == post_owner_id:
            continue

        # Generate generic comment
        comment_text = random.choice(GENERIC_COMMENTS)
        if random.random() > 0.5:
            comment_text += f" {random.choice(EMOJIS)}"

        # Max 2 identical comments per post
        if comment_text_count.get(comment_text, 0) >= 2:
            continue

        comment_data = {
            "user_ref": commenter.reference,
            "username": commenter_name,
            "comment": comment_text,
            "timestamp": datetime.datetime.now(us_tz),
        }

        post_ref.collection("comments").add(comment_data)
        print(f"💬 {commenter_name} commented on post {post_ref.id}: {comment_text}")

        summary_log.append({
            "post_id": post_ref.id,
            "post_owner": post_owner_id,
            "commenter": commenter_name,
            "comment": comment_text
        })

        comment_text_count[comment_text] = comment_text_count.get(comment_text, 0) + 1

# ========== Main Script ==========
def populate_generic_comments_target_folder(folder_path):
    summary_log = []

    # 1. Get target usernames from folder names
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

    # 3. Loop through each target user's posts
    for user_doc in target_users:
        user_id = user_doc.id
        posts = db.collection("posts").where("user_ref", "==", user_doc.reference).stream()
        post_list = list(posts)
        if not post_list:
            continue

        # Other users who can comment
        commenters = [u for u in target_users if u.id != user_id]

        for post in post_list:
            post_ref = post.reference
            add_comments_to_post(post_ref, user_id, commenters, summary_log)

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
    folder_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles"
    populate_generic_comments_target_folder(folder_path)
