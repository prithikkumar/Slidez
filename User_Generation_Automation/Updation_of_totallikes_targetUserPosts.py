import os
import random
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate(
    r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json'
)
firebase_admin.initialize_app(cred)
db = firestore.client()

def update_total_likes(folder_path, min_likes=500, max_likes=10000):
    """
    Randomly update total_likes field for posts of users in the target folder.
    """
    # 1. Get usernames from folder names
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

    # 3. Loop through each user's posts and update total_likes
    for user_doc in target_users:
        user_ref = user_doc.reference
        posts = db.collection("posts").where("user_ref", "==", user_ref).stream()
        for post in posts:
            post_ref = post.reference
            random_likes = random.randint(min_likes, max_likes)
            post_ref.update({"engagement.total_likes": random_likes})
            print(f"🔄 Updated post {post.id} total_likes to {random_likes}")

if __name__ == "__main__":
    folder_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles"  # <-- update your path
    update_total_likes(folder_path)
