import streamlit as st
import numpy as np
import pandas as pd
from scipy.stats import zscore
from googleapiclient.discovery import build
import ace_tools as tools  # Required for displaying tables in Streamlit

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

        video_list.append({"video_id": video_id, "title": title, "channel": channel})

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

# Function to fetch top comments
def get_video_comments(video_id, max_comments=3):
    youtube = get_youtube_service()
    
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
        comments_data = []
        
        for video in videos:
            vid_id = video["video_id"]
            if vid_id in video_stats:
                # Fetch comments
                with st.spinner(f"Fetching comments for {video['title']}..."):
                    comments = get_video_comments(vid_id)

                # Store video data
                data.append({
                    "Title": video["title"],
                    "Channel": video["channel"],
                    "Views": video_stats[vid_id]["views"],
                    "Likes": video_stats[vid_id]["likes"],
                    "Comments": video_stats[vid_id]["comments"],
                    "Video Link": f"https://www.youtube.com/watch?v={vid_id}",
                    "Video ID": vid_id
                })

                # Store comments separately for display
                comments_data.append({
                    "Video ID": vid_id,
                    "Comments": "\n".join(comments) if comments else "No comments available"
                })

        # Convert to DataFrame
        df = pd.DataFrame(data)
        df_comments = pd.DataFrame(comments_data)

        # Compute outlier scores
        outlier_scores = compute_outlier_scores(df["Views"].values)
        df["Outlier Score"] = df["Views"].map(outlier_scores)

        # Sorting options
        sort_by = st.selectbox("Sort videos by:", ["Views", "Outlier Score"], index=0)

        if sort_by == "Views":
            df = df.sort_values(by="Views", ascending=False)
        else:
            df = df.sort_values(by="Outlier Score", ascending=False)

        # Display video data
        tools.display_dataframe_to_user(name="YouTube Search Results", dataframe=df)

        # Display comments separately
        st.subheader("📢 Top Comments for Each Video")
        for _, row in df_comments.iterrows():
            st.markdown(f"**Video:** [{row['Video ID']}](https://www.youtube.com/watch?v={row['Video ID']})")
            st.text_area("Comments", row["Comments"], height=150)

    else:
        st.error("No videos found. Try a different keyword.")
