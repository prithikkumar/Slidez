import os
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

def load_target_usernames_from_folders(folder_path):
    """Load usernames from subfolder names inside folder_path"""
    return {
        name for name in os.listdir(folder_path)
        if os.path.isdir(os.path.join(folder_path, name))
    }

def populate_likes_all_posts(folder_path):
    summary_log = []

    # 1. Load usernames from folder names
    target_usernames = load_target_usernames_from_folders(folder_path)
    print(f"🎯 Target usernames: {target_usernames}")

    if not target_usernames:
        print("⚠️ No target usernames found in the folder.")
        return

    # 2. Fetch users from Firestore matching target usernames
    users = list(db.collection("users").where("username", "in", list(target_usernames)).stream())
    print(f"📦 Found {len(users)} matching users in Firestore.")

    if not users:
        print("⚠️ No matching users found in Firestore.")
        return

    # Map username → user_doc for quick lookup
    username_to_doc = {u.to_dict()["username"]: u for u in users if "username" in u.to_dict()}

    # 3. For each target user, like *all* posts of every other target user
    for liker_doc in users:
        liker_data = liker_doc.to_dict()
        liker_id = liker_doc.id
        liker_name = liker_data.get("username")

        if not liker_name:
            continue

        for owner_name, owner_doc in username_to_doc.items():
            if owner_name == liker_name:
                continue  # Skip self-likes

            posts = db.collection("posts").where("user_ref", "==", owner_doc.reference).stream()
            post_list = list(posts)
            if not post_list:
                continue  # skip if no posts

            for post in post_list:
                post_ref = post.reference

                # Check if already liked
                existing_likes = list(
                    post_ref.collection("like").where("user_ref", "==", liker_doc.reference).stream()
                )
                if existing_likes:
                    continue

                like_data = {
                    "user_ref": liker_doc.reference,
                    "username": liker_name,
                    "timestamp": datetime.datetime.now(us_tz)
                }

                post_ref.collection("like").add(like_data)
                print(f"👍 {liker_name} ({liker_id}) liked post {post_ref.id} by {owner_name}")

                summary_log.append({
                    "post_id": post_ref.id,
                    "post_owner": owner_name,
                    "liker": liker_name,
                    "liker_id": liker_id
                })

    # Summary
    print("\n📊 Summary of Likes Added:")
    for log in summary_log:
        print(f"Post {log['post_id']} by {log['post_owner']}: Liked by {log['liker']} ({log['liker_id']})")

if __name__ == "__main__":
    target_folder = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles"  # folder containing subfolders per username
    populate_likes_all_posts(target_folder)
