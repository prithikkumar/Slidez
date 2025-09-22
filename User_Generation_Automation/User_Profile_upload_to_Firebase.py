import os
import firebase_admin
from firebase_admin import credentials, storage

# 🔑 Initialize Firebase Admin with your service account
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred, {
    "storageBucket": "slidez-be88c.appspot.com"
})

bucket = storage.bucket()

def upload_user_folders(local_base_dir):
    """
    Uploads user folders into Firebase Storage under 'user_profiles/'.
    Skips files that already exist in storage. Logs per-folder and overall summary.
    """
    total_uploaded = 0
    total_skipped = 0

    for username in os.listdir(local_base_dir):
        user_dir = os.path.join(local_base_dir, username)

        if os.path.isdir(user_dir):  # only process folders
            print(f"\n📂 Processing folder: {username}")

            uploaded = 0
            skipped = 0

            for file_name in os.listdir(user_dir):
                local_path = os.path.join(user_dir, file_name)

                if file_name.startswith("."):  # skip hidden files
                    continue

                blob_path = f"user_profiles/{username}/{file_name}"
                blob = bucket.blob(blob_path)

                if blob.exists():
                    print(f"⏩ Skipped (already exists): {blob_path}")
                    skipped += 1
                else:
                    blob.upload_from_filename(local_path)
                    print(f"✅ Uploaded {local_path} → {blob_path}")
                    uploaded += 1

            # Folder summary
            print(f"📊 Summary for {username}: {uploaded} uploaded, {skipped} skipped")

            total_uploaded += uploaded
            total_skipped += skipped

    # Final summary
    print("\n=======================")
    print(f"🏁 Final Summary: {total_uploaded} uploaded, {total_skipped} skipped")
    print("=======================\n")

# 🚀 Run the uploader
upload_user_folders(
    r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\Output_FireStore\user_profiles'
)
