import streamlit as st
import numpy as np
import pandas as pd
from scipy.stats import zscore
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import json

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
        thumbnail = item["snippet"]["thumbnails"]["high"]["url"]

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

# Function to fetch transcript using youtube-transcript-api
def get_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        transcript_text = "\n".join([entry["text"] for entry in transcript])
        return transcript_text
    except (TranscriptsDisabled, NoTranscriptFound):
        return "Transcript is not available for this video."

# Function to fetch top 10 comments
def get_top_comments(video_id, max_comments=50):
    youtube = get_youtube_service()
    
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_comments,
            textFormat="plainText",
            order="relevance"
        )
        response = request.execute()

        if "items" not in response:
            return [{"text": "No comments available.", "likes": 0}]
        
        comments = []
        for item in response["items"]:
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comment_text = snippet.get("textDisplay", "No comment text")
            likes = snippet.get("likeCount", 0)
            
            comments.append({"text": comment_text, "likes": likes})

        top_comments = sorted(comments, key=lambda x: x["likes"], reverse=True)[:10]
        return top_comments if top_comments else [{"text": "No top comments available.", "likes": 0}]
    
    except HttpError as e:
        error_message = e.content.decode("utf-8").lower()
        
        if "disabled comments" in error_message:
            return [{"text": "Comments are disabled for this video.", "likes": 0}]
        elif "quotaexceeded" in error_message:
            return [{"text": "YouTube API quota exceeded. Try again later.", "likes": 0}]
        elif "notfound" in error_message:
            return [{"text": "Video not found or removed.", "likes": 0}]
        else:
            return [{"text": f"Failed to fetch comments: {error_message}", "likes": 0}]

# Function to compute outlier scores
def compute_outlier_scores(view_counts):
    dataset = np.append(np.random.randint(1000, 1000000, size=100), view_counts)
    scores = zscore(dataset)
    
    outlier_dict = {view: round(scores[-len(view_counts) + i], 2) for i, view in enumerate(view_counts)}
    
    return outlier_dict

# Streamlit UI
st.title("🔥 YouTube Video Search & Analysis")

search_query = st.text_input("Enter search keywords:")

if search_query:
    with st.spinner("Searching for videos..."):
        videos = search_videos(search_query)

    if videos:
        video_ids = [video["video_id"] for video in videos]
        
        with st.spinner("Fetching video statistics..."):
            video_stats = get_video_statistics(video_ids)

        data = []
        comments_data = {}
        transcripts_data = {}

        for video in videos:
            vid_id = video["video_id"]
            if vid_id in video_stats:
                with st.spinner(f"Fetching comments for {video['title']}..."):
                    comments = get_top_comments(vid_id)

                with st.spinner(f"Fetching transcript for {video['title']}..."):
                    transcript = get_transcript(vid_id)
                    transcripts_data[vid_id] = transcript

                data.append({
                    "Title": video["title"],
                    "Channel": video["channel"],
                    "Views": video_stats[vid_id]["views"],
                    "Likes": video_stats[vid_id]["likes"],
                    "Video Link": f"https://www.youtube.com/watch?v={vid_id}",
                    "Video ID": vid_id,
                    "Thumbnail": video["thumbnail"]
                })

                comments_data[vid_id] = comments

        df = pd.DataFrame(data)

        outlier_scores = compute_outlier_scores(df["Views"].values)
        df["Outlier Score"] = df["Views"].map(outlier_scores)

        sort_by = st.selectbox("Sort videos by:", ["Views", "Outlier Score"], index=0)

        if sort_by == "Views":
            df = df.sort_values(by="Views", ascending=False)
        else:
            df = df.sort_values(by="Outlier Score", ascending=False)

        st.subheader("📊 YouTube Search Results")

        num_columns = 2
        columns = st.columns(num_columns)

        for index, row in df.iterrows():
            col = columns[index % num_columns]
            with col:
                st.image(row["Thumbnail"], use_container_width=True)
                st.markdown(f"### [{row['Title']}]({row['Video Link']})")
                st.markdown(f"📺 **{row['Channel']}**  |  👍 **{row['Likes']}**  |  👁️ **{row['Views']}** views")
        
                st.markdown("#### 🗨️ Top Comments:")
                comments = comments_data.get(row["Video ID"], [])
                for comment in comments:
                    st.markdown(f"- **{comment['text']}** *(👍 {comment['likes']})*)")

                # **Download Transcript**
                transcript_text = transcripts_data.get(row["Video ID"], "Transcript not available")
                transcript_filename = f"{row['Title'].replace(' ', '_')}_transcript.txt"

                if transcript_text and transcript_text != "Transcript is not available for this video.":
                    transcript_bytes = transcript_text.encode("utf-8")
                    st.download_button(
                        label="📥 Download Transcript",
                        data=transcript_bytes,
                        file_name=transcript_filename,
                        mime="text/plain"
                    )
                else:
                    st.markdown("⚠️ **Transcript not available for this video.**")
