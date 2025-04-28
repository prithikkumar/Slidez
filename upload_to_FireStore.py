import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# Load CSV file (use raw string to avoid backslash issues)
df = pd.read_csv('Slidez_final_1.csv')


# Convert to list of dicts
data = df.to_dict(orient='records')



# Initialize Firebase Admin
cred = credentials.Certificate('slidez_important.json')
firebase_admin.initialize_app(cred)

# Connect to Firestore
db = firestore.client()

# Upload data to a collection called 'products'
for item in data:
    db.collection('products').add(item)

print("Upload complete!")
