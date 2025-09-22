import random
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

def generate_bidirectional_follows():
    users = list(db.collection("users").stream())
    user_data = {user.id: user.to_dict() for user in users}

    follow_map = {uid: {"followers": [], "following": []} for uid in user_data}

    user_ids = list(user_data.keys())

    for uid in user_ids:
        user = user_data[uid]
        possible_targets = [u for u in user_ids if u != uid]
        num_targets = max(1, int(len(possible_targets) * 0.1))  # 30%
        chosen = random.sample(possible_targets, num_targets)

        for target_id in chosen:
            other = user_data[target_id]

            # Following schema for current user
            follow_entry = {
                "id": other["id"] if "id" in other else target_id,
                "image": other.get("avatar", ""),
                "name": other.get("name", ""),
                "requestBy": other.get("username", ""),
                "status": "confirmed"
            }
            if follow_entry not in follow_map[uid]["following"]:
                follow_map[uid]["following"].append(follow_entry)

            # Followers schema for target user
            follower_entry = {
                "id": uid,
                "image": user.get("avatar", ""),
                "name": user.get("name", ""),
                "requestBy": user.get("username", ""),
                "status": "confirmed"
            }
            if follower_entry not in follow_map[target_id]["followers"]:
                follow_map[target_id]["followers"].append(follower_entry)

    # Push updates and prepare summary
    batch = db.batch()
    summary = []

    for uid, data in follow_map.items():
        user_ref = db.collection("users").document(uid)
        batch.update(user_ref, {
            "followers": data["followers"],
            "following": data["following"]
        })
        summary.append({
            "user_id": uid,
            "num_followers": len(data["followers"]),
            "num_following": len(data["following"])
        })

    batch.commit()

    # Print summary
    print("📊 Summary of Bidirectional Follows:")
    for log in summary:
        print(f"User {log['user_id']}: Followers={log['num_followers']}, Following={log['num_following']}")

if __name__ == "__main__":
    generate_bidirectional_follows()
