import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firestore
cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json')  # Replace with your service account JSON
firebase_admin.initialize_app(cred)
db = firestore.client()

def delete_posts_for_users(user_ids):
    """
    Retrieve and delete all posts for given user IDs.
    Returns a dict { user_id: [deleted_post_ids] }
    """
    results = {}
    posts_ref = db.collection("posts")

    for user_id in user_ids:
        try:
            user_ref = db.collection("users").document(user_id)
            query = posts_ref.where("user_ref", "==", user_ref).stream()

            post_ids = [doc.id for doc in query]
            results[user_id] = post_ids

            print(f"\n📌 User {user_id} has {len(post_ids)} posts.")

            # Delete each post
            for pid in post_ids:
                posts_ref.document(pid).delete()
                print(f"❌ Deleted post {pid} for user {user_id}")

        except Exception as e:
            print(f"⚠️ Error processing user {user_id}: {e}")
            results[user_id] = []

    return results


if __name__ == "__main__":
    # Example array of user IDs
    user_ids = [
        "86Ww03Bi8pfydYDseZXRf1aXKj43"
        ,"HbxfeH0cyvZ7OBuXUev4t0XyWw32"
        ,"0ClaBM64t7dmx1TmDtH4QmkeZV92"
        ,"ZBX0qxoafadqxSrgn4IemZqek7z1"
        ,"DzufLXfGIKU3ULginsA30wbhhD93"
        ,"pTDbRZkVH9XbwnhpJIbuXaSwFjb2"
        ,"DoaxnCOjI5VDeFpcqjWAONSlyIa2"
        ,"PScBIfuufwbD61nb0UZtsV6edwJ3"
        ,"YO3W6ozMbQYZcWxLuq3Uo3s3fqv2"
    ]

    deleted_posts = delete_posts_for_users(user_ids)

    print("\n--- Deletion Summary ---")
    for uid, posts in deleted_posts.items():
        print(f"User {uid}: Deleted {len(posts)} posts -> {posts}")
