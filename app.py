import streamlit as st
import pandas as pd
import numpy as np
import json
import datetime
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
            timeframe TEXT,
            video_id TEXT UNIQUE,
            title TEXT,
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

# ✅ Check if data exists in the database
def check_db_for_results(keyword, timeframe):
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT keyword, timeframe, video_id, title, thumbnail, published_date, views, likes, comments, outlier_score FROM search_results WHERE keyword=? AND timeframe=?", (keyword, timeframe))
    results = cursor.fetchall()
    conn.close()
    return results

# ✅ Store new search results in the database
def save_to_db(keyword, timeframe, video_data):
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    for video in video_data:
        cursor.execute("""
            INSERT OR IGNORE INTO search_results (keyword, timeframe, video_id, title, thumbnail, published_date, views, likes, comments, outlier_score) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (keyword, timeframe, video["video_id"], video["title"], video["thumbnail"], video["published_date"],
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
    timeframe = st.radio("Select Timeframe", ["Last 7 Days", "Last 14 Days", "Last 28 Days"])

    days_lookup = {"Last 7 Days": 7, "Last 14 Days": 14, "Last 28 Days": 28}
    days = days_lookup[timeframe]

    keyword = st.text_input("🔎 Enter keyword to search within the niche")

    sort_options = {
        "View Count": "views",
        "Outlier Score": "outlier_score",
        "View-to-Like Ratio": "view_to_like_ratio",
        "View-to-Comment Ratio": "view_to_comment_ratio"
    }
    
    sort_option = st.selectbox("Sort results by", list(sort_options.keys()))

    fetch_button = st.button("Find Outliers")

if fetch_button:
    with st.spinner("Checking database for existing results..."):
        cached_results = check_db_for_results(keyword, timeframe)

    if cached_results:
        st.success(f"✅ Results loaded from database!")

        video_data = pd.DataFrame(cached_results, columns=["keyword", "timeframe", "video_id", "title", "thumbnail", "published_date", "views", "likes", "comments", "outlier_score"])
        
        # Convert numeric columns to int/float
        for col in ["views", "likes", "comments", "outlier_score"]:
            video_data[col] = pd.to_numeric(video_data[col], errors="coerce").fillna(0)
    else:
        with st.spinner("Fetching videos from YouTube..."):
            niche_channels = niche_data[selected_niche]
            all_videos = []

            for channel in niche_channels:
                channel_videos = get_channel_videos(channel["channel_id"], days)
                all_videos.extend(channel_videos)

            video_ids = [video["video_id"] for video in all_videos if video["video_id"]]
            video_stats = get_video_statistics(video_ids)

            view_counts = {vid: stats["views"] for vid, stats in video_stats.items()}
            outlier_scores = compute_outlier_scores(view_counts)

            video_data = []
            for video in all_videos:
                video["views"] = video_stats.get(video["video_id"], {}).get("views", 0)
                video["likes"] = video_stats.get(video["video_id"], {}).get("likes", 0)
                video["comments"] = video_stats.get(video["video_id"], {}).get("comments", 0)
                video["outlier_score"] = outlier_scores.get(video["video_id"], 0)

                video["view_to_like_ratio"] = round(video["views"] / (video["likes"] + 1), 2)
                video["view_to_comment_ratio"] = round(video["views"] / (video["comments"] + 1), 2)

                video_data.append(video)

            video_data = pd.DataFrame(video_data)
            save_to_db(keyword, timeframe, video_data.to_dict(orient="records"))

    # ✅ Fix: Ensure selected column exists before sorting
    selected_column = sort_options[sort_option]
    if selected_column in video_data.columns:
        video_data.sort_values(by=selected_column, ascending=False, inplace=True)

    with col2:
        st.header("📊 Outlier Videos")
        st.write(f"📌 **Total Videos Found: {len(video_data)}**")

        for _, video in video_data.iterrows():
            with st.container():
                colA, colB = st.columns([1, 3])
                with colA:
                    st.image(video["thumbnail"], width=150)
                with colB:
                    st.markdown(f"### [{video['title']}]({'https://www.youtube.com/watch?v=' + video['video_id']})")
                    st.write(f"**Views:** {video['views']:,}")
                    st.write(f"**Likes:** {video['likes']:,}")
                    st.write(f"**Outlier Score:** `{video['outlier_score']}`")
            st.markdown("---")
