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
    cursor.execute("SELECT * FROM search_results WHERE keyword=? AND timeframe=?", (keyword, timeframe))
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

# Fetch videos from a channel within a given timeframe
def get_channel_videos(channel_id, days, max_results=50):
    youtube = get_youtube_service()
    search_date = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat("T") + "Z"
    
    try:
        request = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            publishedAfter=search_date,
            maxResults=max_results,
            type="video",
            order="date"
        )
        response = request.execute()
        
        videos = []
        for item in response.get("items", []):
            if "videoId" in item["id"]:  
                videos.append({
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "thumbnail": item["snippet"]["thumbnails"]["high"]["url"],
                    "published_date": item["snippet"]["publishedAt"]
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

st.markdown("""
    <style>
    body { color: white; background-color: #121212; }
    div[data-testid="stVerticalBlock"] { background-color: #1E1E1E !important; padding: 15px; border-radius: 10px; }
    div[data-testid="stVerticalBlock"] h2 { color: #F0F0F0 !important; }
    .stButton > button { background-color: #FF6B6B !important; color: white !important; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

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
        "View Count": "Views",
        "Outlier Score": "Outlier Score",
        "View-to-Like Ratio": "View-to-Like Ratio",
        "View-to-Comment Ratio": "View-to-Comment Ratio"
    }
    
    sort_option = st.selectbox("Sort results by", list(sort_options.keys()))

    fetch_button = st.button("Find Outliers")

if fetch_button:
    with st.spinner("Checking database for existing results..."):
        cached_results = check_db_for_results(keyword, timeframe)

    if cached_results:
        st.success(f"✅ Results loaded from database!")
        video_data = pd.DataFrame(cached_results, columns=["id", "keyword", "timeframe", "video_id", "title", "thumbnail", "published_date", "views", "likes", "comments", "outlier_score"])
    else:
        with st.spinner("Fetching videos from YouTube..."):
            niche_channels = niche_data[selected_niche]
            all_videos = []
            for channel in niche_channels:
                all_videos.extend(get_channel_videos(channel["channel_id"], days))

            video_ids = [video["video_id"] for video in all_videos if video["video_id"]]
            video_stats = get_video_statistics(video_ids)

            for video in all_videos:
                video.update(video_stats.get(video["video_id"], {}))
                video["outlier_score"] = compute_outlier_scores({video["video_id"]: video.get("views", 0)}).get(video["video_id"], 0)

            video_data = pd.DataFrame(all_videos)
            save_to_db(keyword, timeframe, all_videos)

    video_data.sort_values(by=sort_options[sort_option], ascending=False, inplace=True)
    st.dataframe(video_data[["thumbnail", "title", "views", "likes", "outlier_score"]])
