import os
import firebase_admin
from firebase_admin import credentials, auth, firestore

# Initialize Firebase
cred = credentials.Certificate(
    r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json'
)
firebase_admin.initialize_app(cred)

db = firestore.client()

def get_email_by_username(username):
    """Fetch user's email from Firestore using username"""
    users_ref = db.collection("users")
    query = users_ref.where("username", "==", username).limit(1).stream()
    for doc in query:
        data = doc.to_dict()
        return data.get("email")
    return None

def migrate_user_by_email(email, password):
    try:
        # 1. Find old Firestore document by email
        users_ref = db.collection("users")
        query = users_ref.where("email", "==", email).limit(1).stream()

        old_doc = None
        for doc_snapshot in query:
            old_doc = doc_snapshot
            break

        if not old_doc:
            print(f"⚠️ No Firestore document found for {email}")
            return

        old_data = old_doc.to_dict()

        # 2. Check if user already exists in Firebase Authentication
        try:
            user_record = auth.get_user_by_email(email)
            uid = user_record.uid
            print(f"ℹ️ User {email} already exists in Auth with UID: {uid}")
        except auth.UserNotFoundError:
            # If not, create the user
            user_record = auth.create_user(email=email, password=password)
            uid = user_record.uid
            print(f"✅ Created new Auth user: {email}, UID: {uid}")

        # 3. Check if Firestore doc already exists with UID
        new_doc_ref = db.collection("users").document(uid)
        new_doc_snapshot = new_doc_ref.get()

        if new_doc_snapshot.exists:
            print(f"⏭️ Skipping Firestore doc for {email}, UID already exists")
        else:
            new_doc_ref.set(old_data)
            print(f"📌 Firestore doc created: UID = {uid}, Data = {old_data}")

        # 4. Delete old document if different from UID
        if old_doc.id != uid:
            db.collection("users").document(old_doc.id).delete()
            print(f"🗑️ Deleted old Firestore doc {old_doc.id}")

    except Exception as e:
        print(f"❌ Error migrating {email}: {e}")


if __name__ == "__main__":
    parent_folder_path = r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles"
    usernames = [
        name for name in os.listdir(parent_folder_path)
        if os.path.isdir(os.path.join(parent_folder_path, name))
    ]

    print(f"🎯 Found usernames: {usernames}")

    for username in usernames:
        email = get_email_by_username(username)
        if email:
            migrate_user_by_email(email, "Test@123")
        else:
            print(f"⚠️ Email not found for username '{username}'")
