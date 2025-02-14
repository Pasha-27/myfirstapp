import streamlit as st
import requests
import numpy as np
import pandas as pd
from scipy.stats import zscore
from googleapiclient.discovery import build

# API Key (Replace with your actual API key)
YOUTUBE_API_KEY = "AIzaSyBoDd0TbpH0-NehCVi_QHc4p_lKmjCeIyY"

# Function to extract video ID from URL
def extract_video_id(url):
    if "youtu.be" in url:
        return url.split("/")[-1]
    elif "youtube.com/watch?v=" in url:
        return url.split("v=")[1].split("&")[0]
    return None

# Function to fetch video details
def get_video_data(video_id):
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    
    request = youtube.videos().list(
        part="statistics,snippet",
        id=video_id
    )
    response = request.execute()

    if "items" not in response or not response["items"]:
        return None

    video_data = response["items"][0]
    stats = video_data["statistics"]
    snippet = video_data["snippet"]

    return {
        "title": snippet["title"],
        "views": int(stats.get("viewCount", 0)),
        "likes": int(stats.get("likeCount", 0)),
        "comments": int(stats.get("commentCount", 0)),
        "channel": snippet["channelTitle"]
    }

# Function to fetch comments
def get_video_comments(video_id, max_comments=10):
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=max_comments,
        textFormat="plainText"
    )
    response = request.execute()

    comments = []
    for item in response.get("items", []):
        comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
        comments.append(comment)
    
    return comments

# Function to compute outlier score
def calculate_outlier_score(view_count):
    dataset = np.random.randint(1000, 1000000, size=100)  # Simulated dataset
    dataset = np.append(dataset, view_count)  # Add current video views
    scores = zscore(dataset)
    return round(scores[-1], 2)

# Streamlit UI
st.title("📺 YouTube Video Data Explorer")

# Input box for YouTube URL
video_url = st.text_input("Paste YouTube Video Link:")

if video_url:
    video_id = extract_video_id(video_url)

    if video_id:
        with st.spinner("Fetching video data..."):
            video_info = get_video_data(video_id)
        
        if video_info:
            st.subheader(f"🎬 {video_info['title']}")
            st.write(f"📺 Channel: {video_info['channel']}")
            st.write(f"👀 Views: {video_info['views']:,}")
            st.write(f"👍 Likes: {video_info['likes']:,}")
            st.write(f"💬 Comments: {video_info['comments']:,}")

            # Outlier Score Calculation
            outlier_score = calculate_outlier_score(video_info["views"])
            st.write(f"📊 **Outlier Score:** {outlier_score}")

            # Fetch Comments
            with st.spinner("Fetching comments..."):
                comments = get_video_comments(video_id)
            
            if comments:
                st.subheader("💬 Top Comments:")
                for comment in comments:
                    st.write(f"- {comment}")

        else:
            st.error("Invalid Video URL or Data Not Available")

    else:
        st.error("Invalid YouTube Link")

