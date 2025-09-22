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

def populate_likes(max_likes_per_post=3):
    summary_log = []

    users = list(db.collection("users").stream())
    print(f"📦 Found {len(users)} users in the system.")

    for user_doc in users:
        user_data = user_doc.to_dict()
        user_id = user_doc.id
        user_name = user_data.get("username")
        if not user_name:
            print(f"⚠️ User {user_id} has no username, skipping.")
            continue

        following = user_data.get("following", [])
        if not following:
            continue  # skip users who don't follow anyone

        for follow_entry in following:
            follow_id = follow_entry["id"]
            follow_ref = db.collection("users").document(follow_id)
            follow_doc = follow_ref.get()
            if not follow_doc.exists:
                continue

            follow_data = follow_doc.to_dict()
            posts = db.collection("posts").where("user_ref", "==", follow_ref).stream()
            post_list = list(posts)
            if not post_list:
                continue  # skip if followed user has no posts

            num_posts_to_like = random.randint(1, min(len(post_list), max_likes_per_post))
            selected_posts = random.sample(post_list, num_posts_to_like)

            for post in selected_posts:
                post_ref = post.reference
                # Check if this user already liked this post
                existing_likes = list(post_ref.collection("like").where("user_ref", "==", user_doc.reference).stream())
                if existing_likes:
                    continue

                like_data = {
                    "user_ref": user_doc.reference,
                    "username": user_name,
                    "timestamp": datetime.datetime.now(us_tz)
                }

                post_ref.collection("like").add(like_data)
                print(f"👍 {user_name} ({user_id}) liked post {post_ref.id} by {follow_data.get('username', follow_id)}")

                summary_log.append({
                    "post_id": post_ref.id,
                    "post_owner": follow_data.get("username", follow_id),
                    "liker": user_name,
                    "liker_id": user_id
                })

    # Summary
    print("\n📊 Summary of Likes Added:")
    for log in summary_log:
        print(f"Post {log['post_id']} by {log['post_owner']}: Liked by {log['liker']} ({log['liker_id']})")

if __name__ == "__main__":
    populate_likes()
