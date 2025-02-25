import streamlit as st
import pandas as pd
import numpy as np
import json
import datetime
from scipy.stats import median_abs_deviation
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load API Keys from Streamlit Secrets
YOUTUBE_API_KEY = st.secrets["YOUTUBE_API_KEY"]

# Initialize YouTube API
def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

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

# Apply Dark Mode Styling
st.set_page_config(layout="wide")  

st.markdown(
    """
    <style>
    body {
        color: white;
        background-color: #121212;
    }
    div[data-testid="stVerticalBlock"] {
        background-color: #1E1E1E !important;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0px 2px 10px rgba(255, 255, 255, 0.1);
    }
    div[data-testid="stVerticalBlock"] h2 {
        color: #F0F0F0 !important;
    }
    .stButton > button {
        background-color: #FF6B6B !important;
        color: white !important;
        border-radius: 5px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🎥 YouTube Outlier Video Detector")

col1, col2 = st.columns([1, 2])  

with col1:
    with st.container():
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
    with st.spinner("Fetching videos..."):
        niche_channels = niche_data[selected_niche]
        all_videos = []

        for channel in niche_channels:
            channel_videos = get_channel_videos(channel["channel_id"], days)
            all_videos.extend(channel_videos)

        if not all_videos:
            st.warning("No videos found for this niche in the selected timeframe.")
            st.stop()

    if keyword:
        all_videos = [video for video in all_videos if keyword.lower() in video["title"].lower()]

    video_ids = [video["video_id"] for video in all_videos if video["video_id"]]

    with st.spinner("Fetching video statistics..."):
        video_stats = get_video_statistics(video_ids)

    if not video_stats:
        st.warning("No video statistics found.")
        st.stop()

    view_counts = {vid: stats["views"] for vid, stats in video_stats.items()}
    outlier_scores = compute_outlier_scores(view_counts)

    video_data = []
    for video in all_videos:
        vid_id = video["video_id"]
        if vid_id in video_stats:
            stats = video_stats[vid_id]
            outlier_score = outlier_scores.get(vid_id, 0)

            view_to_like_ratio = round(stats["views"] / (stats["likes"] + 1), 2)
            view_to_comment_ratio = round(stats["views"] / (stats["comments"] + 1), 2)

            video_data.append({
                "Thumbnail": video["thumbnail"],
                "Title": video["title"],
                "Views": stats["views"],
                "Likes": stats["likes"],
                "Outlier Score": outlier_score,
                "View-to-Like Ratio": view_to_like_ratio,
                "View-to-Comment Ratio": view_to_comment_ratio,
                "Video Link": f"https://www.youtube.com/watch?v={vid_id}"
            })

    # Sort the data safely
    video_data.sort(key=lambda x: x.get(sort_options[sort_option], 0), reverse=True)

    with col2:
        st.header("📊 Outlier Videos")

        for video in video_data:
            with st.container():
                colA, colB = st.columns([1, 3])

                with colA:
                    st.image(video["Thumbnail"], width=150)

                with colB:
                    st.markdown(f"### [{video['Title']}]({video['Video Link']})")
                    st.write(f"**Views:** {video['Views']:,}")
                    st.write(f"**Likes:** {video['Likes']:,}")
                    st.write(f"**Outlier Score:** `{video['Outlier Score']}`")
            
            st.markdown("---")
