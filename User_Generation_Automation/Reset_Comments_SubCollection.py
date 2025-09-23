import os
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate(
    r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json'
)
firebase_admin.initialize_app(cred)
db = firestore.client()

def reset_comments_subcollection(folder_path):
    """
    Delete all comments from posts of users whose usernames are in the target folder.
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

    # 3. Loop through each user's posts and delete comments
    for user_doc in target_users:
        user_ref = user_doc.reference
        posts = db.collection("posts").where("user_ref", "==", user_ref).stream()
        for post in posts:
            post_ref = post.reference
            comment_docs = list(post_ref.collection("comments").stream())
            count = 0
            for comment_doc in comment_docs:
                comment_doc.reference.delete()
                count += 1
            print(f"🗑 Cleared {count} comments from post: {post.id}")

if __name__ == "__main__":
    folder_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles"  # <-- update your path
    reset_comments_subcollection(folder_path)
