import streamlit as st
import numpy as np
import pandas as pd
from scipy.stats import zscore
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# API Key (Replace with your actual API key)
YOUTUBE_API_KEY = "AIzaSyBoDd0TbpH0-NehCVi_QHc4p_lKmjCeIyY"

# Initialize YouTube API
def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# Function to search videos based on a keyword
def search_videos(query, max_results=10):
    youtube = get_youtube_service()
    
    request = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=max_results
    )
    response = request.execute()

    video_list = []
    
    for item in response.get("items", []):
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        channel = item["snippet"]["channelTitle"]
        thumbnail = item["snippet"]["thumbnails"]["high"]["url"]  # Get high-resolution thumbnail

        video_list.append({"video_id": video_id, "title": title, "channel": channel, "thumbnail": thumbnail})

    return video_list

# Function to fetch video statistics
def get_video_statistics(video_ids):
    youtube = get_youtube_service()
    
    request = youtube.videos().list(
        part="statistics",
        id=",".join(video_ids)
    )
    response = request.execute()

    video_stats = {}
    
    for item in response.get("items", []):
        video_id = item["id"]
        stats = item["statistics"]
        
        video_stats[video_id] = {
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
        }

    return video_stats

# Function to compute outlier scores
def compute_outlier_scores(view_counts):
    dataset = np.append(np.random.randint(1000, 1000000, size=100), view_counts)
    scores = zscore(dataset)
    
    # Map scores back to the original data
    outlier_dict = {view: round(scores[-len(view_counts) + i], 2) for i, view in enumerate(view_counts)}
    
    return outlier_dict

# Streamlit UI
st.title("🔥 YouTube Video Search & Analysis")

# Input box for keywords
search_query = st.text_input("Enter search keywords:")

if search_query:
    with st.spinner("Searching for videos..."):
        videos = search_videos(search_query)

    if videos:
        video_ids = [video["video_id"] for video in videos]
        
        with st.spinner("Fetching video statistics..."):
            video_stats = get_video_statistics(video_ids)

        # Merge video details with statistics
        data = []
        
        for video in videos:
            vid_id = video["video_id"]
            if vid_id in video_stats:
                data.append({
                    "Title": video["title"],
                    "Channel": video["channel"],
                    "Views": video_stats[vid_id]["views"],
                    "Likes": video_stats[vid_id]["likes"],
                    "Video Link": f"https://www.youtube.com/watch?v={vid_id}",
                    "Video ID": vid_id,
                    "Thumbnail": video["thumbnail"]  # Store thumbnail
                })

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Compute outlier scores
        outlier_scores = compute_outlier_scores(df["Views"].values)
        df["Outlier Score"] = df["Views"].map(outlier_scores)

        # Sorting options
        sort_by = st.selectbox("Sort videos by:", ["Views", "Outlier Score"], index=0)

        if sort_by == "Views":
            df = df.sort_values(by="Views", ascending=False)
        else:
            df = df.sort_values(by="Outlier Score", ascending=False)

        # **🎨 Display results in Gallery View**
        st.subheader("📊 YouTube Search Results")

        num_columns = 3  # Set the number of columns for the gallery view
        columns = st.columns(num_columns)

        for index, row in df.iterrows():
            col = columns[index % num_columns]  # Distribute videos across columns
            with col:
                st.image(row["Thumbnail"], use_column_width=True)
                st.markdown(f"**[{row['Title']}]({row['Video Link']})**")
                st.markdown(f"📺 {row['Channel']}  |  👍 {row['Likes']}  |  👁️ {row['Views']} views")
                st.markdown("---")  # Divider for readability

    else:
        st.error("No videos found. Try a different keyword.")
