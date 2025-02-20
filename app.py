import streamlit as st
import numpy as np
import pandas as pd
from scipy.stats import zscore
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import openai

# OpenAI API Key (Loaded securely)
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
YOUTUBE_API_KEY = st.secrets["YOUTUBE_API_KEY"]

# Initialize OpenAI Client
client = openai.OpenAI(api_key=OPENAI_API_KEY)

view_counts = [video_stats[vid_id]["views"] for vid_id in video_ids]
outlier_scores = compute_outlier_scores(view_counts)

# Initialize YouTube API
def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def compute_outlier_scores(view_counts):
    """Computes Z-score based outlier scores for views."""
    if len(view_counts) < 2:
        return {view: 0 for view in view_counts}  # Avoid division by zero if only one video

    scores = zscore(view_counts)  # Compute Z-scores
    outlier_dict = {view_counts[i]: round(scores[i], 2) for i in range(len(view_counts))}

    return outlier_dict

# Function to search videos
def search_videos(query, max_results=10):
    youtube = get_youtube_service()
    request = youtube.search().list(
        q=query, part="snippet", type="video", maxResults=max_results
    )
    response = request.execute()
    
    return [
        {"video_id": item["id"]["videoId"], 
         "title": item["snippet"]["title"], 
         "channel": item["snippet"]["channelTitle"], 
         "thumbnail": item["snippet"]["thumbnails"]["high"]["url"]}
        for item in response.get("items", [])
    ]

# Function to fetch video statistics
def get_video_statistics(video_ids):
    youtube = get_youtube_service()
    request = youtube.videos().list(
        part="statistics", id=",".join(video_ids)
    )
    response = request.execute()

    return {
        item["id"]: {
            "views": int(item["statistics"].get("viewCount", 0)),
            "likes": int(item["statistics"].get("likeCount", 0)),
            "comments": int(item["statistics"].get("commentCount", 0)),
        }
        for item in response.get("items", [])
    }

# Function to fetch transcript
def get_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return "\n".join([entry["text"] for entry in transcript])
    except (TranscriptsDisabled, NoTranscriptFound):
        return "Transcript is not available for this video."

# Function to fetch top 10 comments
def get_top_comments(video_id, max_comments=50):
    youtube = get_youtube_service()
    
    try:
        request = youtube.commentThreads().list(
            part="snippet", videoId=video_id, maxResults=max_comments, 
            textFormat="plainText", order="relevance"
        )
        response = request.execute()

        if "items" not in response:
            return [{"text": "No comments available.", "likes": 0}]
        
        comments = [
            {"text": item["snippet"]["topLevelComment"]["snippet"].get("textDisplay", "No comment text"),
             "likes": item["snippet"]["topLevelComment"]["snippet"].get("likeCount", 0)}
            for item in response["items"]
        ]
        
        return sorted(comments, key=lambda x: x["likes"], reverse=True)[:10]
    
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

# Function to analyze common patterns using ChatGPT
def analyze_patterns(transcript, comments):
    """Sends transcript and comments to ChatGPT for pattern detection."""
    if transcript == "Transcript is not available for this video.":
        return "Transcript not available for analysis."
    
    comments_text = "\n".join([comment["text"] for comment in comments])

    # Limit the length to avoid API issues
    transcript = transcript[:9000]  # First 9000 characters
    comments_text = comments_text[:2000]  # First 2000 characters

    prompt = f"""
    Compare the following video transcript with its comments. Identify common themes, recurring phrases, emotions, and engagement patterns.

    Transcript (truncated):
    {transcript}

    Comments (truncated):
    {comments_text}

    Provide a summary of common topics, user sentiment, and any unique insights.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content

    except openai.OpenAIError as e:
        return f"⚠️ OpenAI API Error: {str(e)}"

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

        for video in videos:
        vid_id = video["video_id"]
        if vid_id in video_stats:
            comments = get_top_comments(vid_id)
            transcript = get_transcript(vid_id)

            st.image(video["thumbnail"], use_container_width=True)
            st.markdown(f"### [{video['title']}]({video['video_id']})")
            st.markdown(f"📺 **{video['channel']}**  |  👍 **{video_stats[vid_id]['likes']}**  |  👁️ **{video_stats[vid_id]['views']}** views")

        # Display Outlier Score
            outlier_score = outlier_scores.get(video_stats[vid_id]["views"], 0)
            st.markdown(f"🔍 **Outlier Score:** `{outlier_score}`")

            st.markdown("#### 🗨️ Top Comments:")
            for comment in comments:
                st.markdown(f"- **{comment['text']}** *(👍 {comment['likes']})*)")

            if transcript != "Transcript is not available for this video.":
                st.download_button(
                    label="📥 Download Transcript",
                    data=transcript.encode("utf-8"),
                    file_name=f"{video['title']}_transcript.txt",
                    mime="text/plain"
            )

                if st.button(f"🧠 Analyze Patterns", key=vid_id):
                    with st.spinner("Analyzing patterns..."):
                        analysis_result = analyze_patterns(transcript, comments)
                        analysis_filename = f"{video['title']}_patterns.txt"

                        st.download_button(
                            label="📥 Download Analysis",
                            data=analysis_result.encode("utf-8"),
                            file_name=analysis_filename,
                            mime="text/plain"
                        )
            else:
                st.markdown("⚠️ **Transcript not available for this video.**")
