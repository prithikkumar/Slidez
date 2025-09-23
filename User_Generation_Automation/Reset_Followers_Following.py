import firebase_admin
from firebase_admin import credentials, firestore

# 🔑 Initialize Firebase
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

def clear_followers_following():
    users_ref = db.collection("users")
    docs = users_ref.stream()

    for doc in docs:
        user_id = doc.id
        data = doc.to_dict()
        username = data.get("username", "unknown")

        # Clear followers and following arrays
        users_ref.document(user_id).update({
            "followers": [],
            "following": []
        })

        print(f"🧹 Cleared followers & following for {username} ({user_id})")

if __name__ == "__main__":
    clear_followers_following()
    print("\n✅ All users now have empty 'followers' and 'following' arrays.")
