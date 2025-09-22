import firebase_admin
from firebase_admin import credentials, firestore

# 🔑 Initialize Firebase
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

def get_user_doc(username):
    """Fetch a user's Firestore document by username."""
    users_ref = db.collection("users")
    query = users_ref.where("username", "==", username).limit(1).stream()
    for doc in query:
        return doc.id, doc.to_dict()
    return None, None

def merge_entries(existing_list, new_entries):
    """Merge new entries into existing list, avoiding duplicates by 'Id'."""
    existing_ids = {item["id"] for item in existing_list}
    merged = existing_list.copy()
    for entry in new_entries:
        if entry["id"] not in existing_ids:
            merged.append(entry)
    return merged

def generate_bidirectional_followers(target_usernames):
    # Map of username -> {id, data}
    user_map = {}
    for username in target_usernames:
        doc_id, data = get_user_doc(username)
        if not doc_id:
            print(f"⚠️ Username '{username}' not found in Firestore.")
            continue
        user_map[username] = {"id": doc_id, "data": data}

    # Now create bidirectional following/followers
    for username, user_info in user_map.items():
        user_id = user_info["id"]
        user_data = user_info["data"]

        # Existing arrays or empty
        current_following = user_data.get("following", [])
        current_followers = user_data.get("followers", [])

        user_following = []
        user_followers = []

        for other_username, other_info in user_map.items():
            if other_username == username:
                continue  # skip self

            # following entry
            following_entry = {
                "id": other_info["id"],
                "image": other_info["data"].get("avatar", ""),   # avatar image
                "name": other_info["data"].get("name", other_username),
                "requestBy": username,
                "status": "confirmed"
            }
            user_following.append(following_entry)

            # follower entry
            follower_entry = {
                "id": other_info["id"],
                "image": other_info["data"].get("avatar", ""),
                "name": other_info["data"].get("name", other_username),
                "requestBy": other_username,
                "status": "confirmed"
            }
            user_followers.append(follower_entry)

        # Merge with existing
        updated_following = merge_entries(current_following, user_following)
        updated_followers = merge_entries(current_followers, user_followers)

        # Update Firestore
        db.collection("users").document(user_id).update({
            "following": updated_following,
            "followers": updated_followers
        })

        # Logging
        print(f"\n👤 User: {username} ({user_id})")
        print(f"   ➡️ Following: {[f['name'] for f in updated_following]}")
        print(f"   ⬅️ Followers: {[f['name'] for f in updated_followers]}")

if __name__ == "__main__":
    # 🎯 Target usernames you want to connect with each other
    target_usernames = ["Sophia", "Madeline", "Noah"]

    generate_bidirectional_followers(target_usernames)
    print("\n✅ Bidirectional followers/following merged for target usernames.")
