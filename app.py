import streamlit as st
import pandas as pd
import numpy as np
import json
import sqlite3
from scipy.stats import median_abs_deviation
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load API Keys from Streamlit Secrets
YOUTUBE_API_KEY = st.secrets["YOUTUBE_API_KEY"]

# Initialize YouTube API
def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# ✅ Initialize SQLite Database
def initialize_db():
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT,
            video_id TEXT UNIQUE,
            title TEXT,
            description TEXT,
            thumbnail TEXT,
            published_date TEXT,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            outlier_score REAL
        )
    """)
    conn.commit()
    conn.close()

# ✅ Clear Cache (Delete all records from the database)
def clear_cache():
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM search_results")  # Deletes all stored results
    conn.commit()
    conn.close()

# ✅ Check if data exists in the database
def check_db_for_results(keyword):
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT keyword, video_id, title, description, thumbnail, published_date, views, likes, comments, outlier_score FROM search_results WHERE keyword=?", (keyword,))
    results = cursor.fetchall()
    conn.close()
    return results

# ✅ Store new search results in the database
def save_to_db(keyword, video_data):
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    for video in video_data:
        cursor.execute("""
            INSERT OR IGNORE INTO search_results (keyword, video_id, title, description, thumbnail, published_date, views, likes, comments, outlier_score) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (keyword, video["video_id"], video["title"], video["description"], video["thumbnail"], video["published_date"],
             video["views"], video["likes"], video["comments"], video["outlier_score"])
        )
    conn.commit()
    conn.close()

# Load Niche Channels
def load_niche_channels():
    with open("channels.json", "r") as f:
        return json.load(f)

# Compute outlier scores using Modified Z-Score
def compute_outlier_scores(view_counts):
    if not view_counts:
        return {}

    view_list = list(view_counts.values())

    if len(view_list) < 2:
        return {vid: 0 for vid in view_counts}

    median_views = np.median(view_list)
    mad = median_abs_deviation(view_list)

    if mad == 0:
        return {vid: 0 for vid in view_counts}

    scores = [0.6745 * (view - median_views) / mad for view in view_list]

    return {list(view_counts.keys())[i]: round(scores[i], 2) for i in range(len(view_list))}

# ✅ Fetch all videos from a channel (Lifetime)
def get_channel_videos(channel_id, max_results=50):
    youtube = get_youtube_service()

    try:
        request = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            maxResults=max_results,
            type="video",
            order="date"
        )
        response = request.execute()

        videos = []
        for item in response.get("items", []):
            if "videoId" in item["id"]:
                video_id = item["id"]["videoId"]
                title = item["snippet"]["title"]
                description = item["snippet"].get("description", "")  # ✅ Fetch description
                thumbnail = item["snippet"]["thumbnails"]["high"]["url"]
                published_date = item["snippet"]["publishedAt"]

                videos.append({
                    "video_id": video_id,
                    "title": title,
                    "description": description,
                    "thumbnail": thumbnail,
                    "published_date": published_date
                })
        return videos

    except HttpError as e:
        st.error(f"API Error: {e}")
        return []

# Fetch video statistics
def get_video_statistics(video_ids):
    youtube = get_youtube_service()

    if not video_ids:
        return {}

    video_stats = {}

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]

        try:
            request = youtube.videos().list(
                part="statistics",
                id=",".join(chunk)
            )
            response = request.execute()

            for item in response.get("items", []):
                video_stats[item["id"]] = {
                    "views": int(item["statistics"].get("viewCount", 0)),
                    "likes": int(item["statistics"].get("likeCount", 0)),
                    "comments": int(item["statistics"].get("commentCount", 0)),
                }

        except HttpError as e:
            st.error(f"API Error: {e}")

    return video_stats

# ✅ Initialize Database
initialize_db()

# Apply Dark Mode Styling
st.set_page_config(layout="wide")  

st.title("🎥 YouTube Outlier Video Detector")

col1, col2 = st.columns([1, 2])  

with col1:
    st.header("🔍 Filter Options")

    niche_data = load_niche_channels()
    niches = list(niche_data.keys())

    selected_niche = st.selectbox("Select a Niche", niches)
    keyword = st.text_input("🔎 Enter keyword to search within the niche")

    sort_options = {
        "View Count": "views",
        "Outlier Score": "outlier_score"
    }
    
    sort_option = st.selectbox("Sort results by", list(sort_options.keys()))

    fetch_button = st.button("Find Outliers")

    # ✅ Add Clear Cache Button
    if st.button("🗑️ Clear Cache"):
        clear_cache()
        st.success("Cache cleared! The database has been reset.")

if fetch_button:
    with st.spinner("Checking database for existing results..."):
        cached_results = check_db_for_results(keyword)

    if cached_results:
        st.success(f"✅ Results loaded from database!")

        video_data = pd.DataFrame(cached_results, columns=["keyword", "video_id", "title", "description", "thumbnail", "published_date", "views", "likes", "comments", "outlier_score"])

        for col in ["views", "likes", "comments", "outlier_score"]:
            video_data[col] = pd.to_numeric(video_data[col], errors="coerce").fillna(0)

        video_data = video_data[
            ((video_data["title"].str.contains(keyword, case=False, na=False)) |
             (video_data["description"].str.contains(keyword, case=False, na=False))) &
            (video_data["outlier_score"] > 5)
        ]
    else:
        with st.spinner("Fetching videos from YouTube..."):
            niche_channels = niche_data[selected_niche]
            all_videos = []
            for channel in niche_channels:
                all_videos.extend(get_channel_videos(channel["channel_id"]))

            video_ids = [video["video_id"] for video in all_videos if video["video_id"]]
            video_stats = get_video_statistics(video_ids)

            view_counts = {vid: stats["views"] for vid, stats in video_stats.items()}
            outlier_scores = compute_outlier_scores(view_counts)

            for video in all_videos:
                video["outlier_score"] = outlier_scores.get(video["video_id"], 0)

            save_to_db(keyword, all_videos)

    video_data.sort_values(by=sort_options[sort_option], ascending=False, inplace=True)
    st.dataframe(video_data)
