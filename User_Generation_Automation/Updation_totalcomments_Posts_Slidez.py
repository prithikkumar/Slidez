import firebase_admin
from firebase_admin import credentials, firestore

# ========== Firebase Setup ==========
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ========== Update total_comments ==========
def update_total_comments_posts():
    posts_docs = db.collection("posts").stream()
    for post in posts_docs:
        post_ref = post.reference
        comments = list(post_ref.collection("comments").stream())
        total_comments = len(comments)

        # Update engagement.total_comments
        post_ref.update({"engagement.total_comments": total_comments})
        print(f"🔄 Updated post {post.id} total_comments = {total_comments}")

def update_total_comments_slidez():
    posts_docs = db.collection("slidez").stream()
    for post in posts_docs:
        post_ref = post.reference
        comments = list(post_ref.collection("comments").stream())
        total_comments = len(comments)

        # Update engagement.total_comments
        post_ref.update({"engagement.total_comments": total_comments})
        print(f"🔄 Updated post {post.id} total_comments = {total_comments}")

if __name__ == "__main__":
    update_total_comments_posts()
    update_total_comments_slidez()
