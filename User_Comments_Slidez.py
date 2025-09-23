import random
import datetime
import pytz
import firebase_admin
from firebase_admin import credentials, firestore

# ========== Firebase Setup ==========
cred = credentials.Certificate(
    r"C:\Users\prith\OneDrive\Desktop\Slidez\FireStorePush\User_Generation_Automation\slidez-be88c-firebase-adminsdk-lduy4-a1a064fc0c.json"
)
firebase_admin.initialize_app(cred)
db = firestore.client()

# US timezone
us_tz = pytz.timezone("US/Eastern")

# ========== Generic Social Media Marketing Comments ==========
GENERIC_COMMENTS = [
    "🔥 Loving this campaign!", 
    "💡 Great strategy here!", 
    "👏 Nice engagement idea!", 
    "📈 This is really effective!", 
    "😍 Amazing content!", 
    "👍 Definitely shareable!", 
    "💯 Impressive reach potential!", 
    "💡 Clever approach!", 
    "🎯 Perfect targeting!", 
    "💬 This would spark great conversation!", 
    "📸 Really eye-catching visuals!", 
    "✨ Creative post!", 
    "🙌 Love it!", 
    "🎉 Exciting idea!", 
    "💥 Strong messaging!", 
    "😎 Such a vibe for social media!", 
    "💫 Engaging and trendy!", 
    "❤️ This would get a lot of likes!", 
    "📣 Very shareable content!", 
    "👏 Nicely executed!"
]

EMOJIS = ["🔥","😍","😎","👍","💯","😮","📸","✨","🤩","😲","😃","❤️","👏","🎉","💡","🙌","🎯","💥","💫"]

# ========== Helper Function ==========
def add_comments_to_slidez_doc(doc_ref, doc_owner_ref, summary_log):
    """Add generic comments from followers of the doc owner."""
    # Get followers of the owner
    owner_doc = doc_owner_ref.get()
    if not owner_doc.exists:
        return

    owner_data = owner_doc.to_dict()
    followers = owner_data.get("followers", [])  # array of dicts with 'id' field
    if not followers:
        return

    # Fetch follower user documents safely
    follower_docs = []
    for f in followers:
        follower_id = f.get("id")
        if not follower_id:
            continue  # skip invalid follower
        try:
            follower_doc = db.collection("users").document(follower_id).get()
            if follower_doc.exists:
                follower_docs.append(follower_doc)
        except Exception as e:
            print(f"⚠️ Could not fetch follower {follower_id}: {e}")

    if not follower_docs:
        return

    # Existing comments
    existing_comments_docs = doc_ref.collection("comments").stream()
    existing_commenters = {doc.to_dict().get("username") for doc in existing_comments_docs}
    comment_text_count = {}
    for doc in existing_comments_docs:
        text = doc.to_dict().get("comment")
        if text:
            comment_text_count[text] = comment_text_count.get(text, 0) + 1

    for follower_doc in follower_docs:
        follower_data = follower_doc.to_dict()
        username = follower_data.get("username", "anon")

        if username in existing_commenters:
            continue  # skip if already commented

        # Generate comment
        comment_text = random.choice(GENERIC_COMMENTS)
        if random.random() > 0.5:
            comment_text += f" {random.choice(EMOJIS)}"

        # Max 2 identical comments per doc
        if comment_text_count.get(comment_text, 0) >= 2:
            continue

        comment_data = {
            "user_ref": follower_doc.reference,
            "username": username,
            "comment": comment_text,
            "timestamp": datetime.datetime.now(us_tz),
        }

        doc_ref.collection("comments").add(comment_data)
        print(f"💬 {username} commented on slidez doc {doc_ref.id}: {comment_text}")

        summary_log.append({
            "doc_id": doc_ref.id,
            "doc_owner": doc_owner_ref.id,
            "commenter": username,
            "comment": comment_text
        })

        comment_text_count[comment_text] = comment_text_count.get(comment_text, 0) + 1

# ========== Main Script ==========
def populate_comments_for_slidez():
    summary_log = []

    # Fetch all slidez documents
    slidez_docs = list(db.collection("slidez").stream())
    if not slidez_docs:
        print("⚠️ No documents found in slidez collection.")
        return

    print(f"📄 Found {len(slidez_docs)} documents in slidez collection.")

    for doc in slidez_docs:
        doc_ref = doc.reference
        doc_data = doc.to_dict()
        doc_owner_ref = doc_data.get("user_ref")
        if not doc_owner_ref:
            continue
        add_comments_to_slidez_doc(doc_ref, doc_owner_ref, summary_log)

    # ========== Summary ==========
    print("\n📊 Summary of Comments Added:")
    summary_count = {}
    for log in summary_log:
        key = log["doc_id"]
        summary_count.setdefault(key, {"doc_owner": log["doc_owner"], "comments": []})
        summary_count[key]["comments"].append({
            "commenter": log["commenter"],
            "comment": log["comment"]
        })

    for doc_id, data in summary_count.items():
        comment_texts = ", ".join([f"{c['commenter']}: \"{c['comment']}\"" for c in data["comments"]])
        print(f"Slidez doc {doc_id} by {data['doc_owner']}: {len(data['comments'])} comments -> {comment_texts}")

# ========== Run ==========
if __name__ == "__main__":
    populate_comments_for_slidez()
