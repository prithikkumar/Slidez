import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json')
firebase_admin.initialize_app(cred)

db = firestore.client()

def reset_likes_subcollection():
    posts = db.collection("posts").stream()
    for post in posts:
        post_ref = post.reference
        like_docs = post_ref.collection("likes").stream()
        count = 0
        for like_doc in like_docs:
            like_doc.reference.delete()
            count += 1
        print(f"🗑 Cleared {count} likes from post: {post.id}")

if __name__ == "__main__":
    reset_likes_subcollection()
