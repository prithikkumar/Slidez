import os
import firebase_admin
from firebase_admin import credentials, storage

# 🔑 Initialize Firebase Admin with your service account
cred = credentials.Certificate(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json')
firebase_admin.initialize_app(cred, {
    "storageBucket": "slidez-be88c.appspot.com"
})

bucket = storage.bucket()

def upload_user_folders(local_base_dir):
    """
    Uploads all user folders from local directory into
    Firebase Storage under 'user_profiles/' prefix.
    
    Example structure in Storage:
      user_profiles/prithik/profile.jpg
      user_profiles/prithik/post1.png
      user_profiles/john_doe/avatar.png
    """
    for username in os.listdir(local_base_dir):
        user_dir = os.path.join(local_base_dir, username)

        if os.path.isdir(user_dir):  # only process folders
            print(f"📂 Uploading folder: {username}")

            for file_name in os.listdir(user_dir):
                local_path = os.path.join(user_dir, file_name)

                # Skip hidden files (like .DS_Store on Mac)
                if file_name.startswith("."):
                    continue

                # ✅ Add "user_profiles/" prefix here
                blob_path = f"user_profiles/{username}/{file_name}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_path)

                print(f"✅ Uploaded {local_path} → {blob_path}")

# 🚀 Run the uploader
upload_user_folders(r'C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\user_profiles') # replace with your local directory
