import streamlit as st
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
import os
from scipy.stats import median_abs_deviation
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import openai

# Load API Keys from Streamlit Secrets
YOUTUBE_API_KEY = st.secrets["YOUTUBE_API_KEY"]
openai.api_key = st.secrets["OPENAI_API_KEY"]

# Function to fetch top 50 comments for a given video
def get_video_comments(video_id, max_results=50):
    youtube = get_youtube_service()
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_results,
            textFormat="plainText"
        )
        response = request.execute()
        items = response.get("items", [])
        for item in items:
            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comments.append(comment)
    except HttpError as e:
        st.error(f"API Error when fetching comments for video {video_id}: {e}")
    return comments

# Function to analyze sentiment for a list of comments using OpenAI API
def analyze_sentiment_for_comments(comments):
    if not comments:
        return {"positive": [], "neutral": [], "negative": []}
    # Create a prompt that lists the comments and asks for a JSON output
    prompt = (
        "Please classify the following YouTube comments into positive, neutral, and negative categories. "
        "Provide the output as a JSON object with keys 'positive', 'neutral', and 'negative', each containing "
        "a list of comments that fall into that category. Here are the comments:\n\n" +
        "\n".join(f"- {comment}" for comment in comments)
    )
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that classifies text sentiment."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        output_text = response.choices[0].message.content.strip()
        sentiment_dict = json.loads(output_text)
        # Ensure all keys exist
        for key in ["positive", "neutral", "negative"]:
            if key not in sentiment_dict:
                sentiment_dict[key] = []
        return sentiment_dict
    except Exception as e:
        st.error("Error during sentiment analysis: " + str(e))
        return {"positive": [], "neutral": [], "negative": []}

# Initialize YouTube API
def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# Improved Database Schema
def initialize_db():
    db_exists = os.path.exists("youtube_data.db")
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    
    if not db_exists:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT UNIQUE,
                channel_id TEXT,
                channel_name TEXT,
                title TEXT,
                description TEXT,
                thumbnail TEXT,
                published_date TEXT,
                fetch_date TEXT,
                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                outlier_score REAL DEFAULT 0
            )
        """)
    else:
        cursor.execute("PRAGMA table_info(search_results)")
        existing_columns = [column[1] for column in cursor.fetchall()]
        expected_columns = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "video_id": "TEXT UNIQUE",
            "channel_id": "TEXT",
            "channel_name": "TEXT",
            "title": "TEXT",
            "description": "TEXT",
            "thumbnail": "TEXT",
            "published_date": "TEXT",
            "fetch_date": "TEXT",
            "views": "INTEGER DEFAULT 0",
            "likes": "INTEGER DEFAULT 0",
            "comments": "INTEGER DEFAULT 0",
            "outlier_score": "REAL DEFAULT 0"
        }
        for column, column_type in expected_columns.items():
            if column not in existing_columns and column != "id":
                try:
                    cursor.execute(f"ALTER TABLE search_results ADD COLUMN {column} {column_type}")
                    st.info(f"Added missing column: {column}")
                except sqlite3.Error as e:
                    st.error(f"Error adding column {column}: {e}")
    
    conn.commit()
    conn.close()

# Clear Cache (Delete all records from the database)
def clear_cache():
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM search_results")
    conn.commit()
    conn.close()

# Fixed database search function with duplicate removal
def search_db_results(niche=None, keyword=None, min_outlier_score=None, sort_by="views", niche_data=None):
    conn = sqlite3.connect("youtube_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query_parts = ["""
        SELECT video_id, channel_id, channel_name, title, description, thumbnail, 
               published_date, fetch_date, views, likes, comments, outlier_score 
        FROM search_results
    """]
    
    params = []
    where_conditions = []
    
    if niche and niche_data and niche in niche_data:
        channel_ids = [channel["channel_id"] for channel in niche_data[niche]]
        if channel_ids:
            channel_placeholders = ",".join(["?"] * len(channel_ids))
            where_conditions.append(f"channel_id IN ({channel_placeholders})")
            params.extend(channel_ids)
    
    if keyword:
        where_conditions.append("(title LIKE ? OR description LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    
    if min_outlier_score is not None:
        where_conditions.append("outlier_score >= ?")
        params.append(min_outlier_score)
    
    if where_conditions:
        query_parts.append("WHERE " + " AND ".join(where_conditions))
    
    if sort_by:
        query_parts.append(f"ORDER BY {sort_by} DESC")
    
    final_query = " ".join(query_parts)
    
    try:
        cursor.execute(final_query, params)
        results = cursor.fetchall()
        result_list = [dict(row) for row in results]
        # Remove duplicates by video_id
        unique_results = list({video["video_id"]: video for video in result_list}.values())
        conn.close()
        return unique_results
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        conn.close()
        return []

# Improved save to db function
def save_to_db(video_data):
    if not video_data:
        return
        
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    
    current_date = datetime.datetime.now().isoformat()
    
    for video in video_data:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO search_results (
                    video_id, channel_id, channel_name, title, description, thumbnail, 
                    published_date, fetch_date, views, likes, comments, outlier_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                video["video_id"],
                video.get("channel_id", ""),
                video.get("channel_name", ""),
                video["title"],
                video.get("description", ""),
                video.get("thumbnail", ""),
                video.get("published_date", ""),
                current_date,
                video.get("views", 0),
                video.get("likes", 0),
                video.get("comments", 0),
                video.get("outlier_score", 0)
            ))
        except sqlite3.Error as e:
            st.error(f"Error saving video {video.get('title', 'unknown')}: {e}")
    
    conn.commit()
    conn.close()

# Load Niche Channels with error handling
def load_niche_channels():
    try:
        with open("channels.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("channels.json file not found!")
        return {}
    except json.JSONDecodeError:
        st.error("Error parsing channels.json - invalid format!")
        return {}

# Compute outlier scores using Modified Z-Score
def compute_outlier_scores(videos, metric="views"):
    if not videos:
        return videos
    
    values = {video["video_id"]: video.get(metric, 0) for video in videos}
    
    if len(values) < 2:
        for video in videos:
            video["outlier_score"] = 0
        return videos
    
    metric_list = list(values.values())
    median_value = np.median(metric_list)
    mad = median_abs_deviation(metric_list)
    
    if mad == 0:
        for video in videos:
            video["outlier_score"] = 0
        return videos
    
    scores = {vid: 0.6745 * (value - median_value) / mad for vid, value in values.items()}
    
    for video in videos:
        video["outlier_score"] = round(scores.get(video["video_id"], 0), 2)
    
    return videos

# Fetch all videos from a channel with pagination
def get_channel_videos(channel_id, channel_name, max_results=100):
    youtube = get_youtube_service()
    videos = []
    next_page_token = None
    total_results = 0
    
    try:
        while total_results < max_results:
            batch_size = min(50, max_results - total_results)
            request_params = {
                "part": "id,snippet",
                "channelId": channel_id,
                "maxResults": batch_size,
                "type": "video",
                "order": "date"
            }
            
            if next_page_token:
                request_params["pageToken"] = next_page_token
                
            request = youtube.search().list(**request_params)
            response = request.execute()
            
            items = response.get("items", [])
            if not items:
                break
                
            for item in items:
                if "videoId" in item["id"]:
                    video_id = item["id"]["videoId"]
                    title = item["snippet"]["title"]
                    description = item["snippet"].get("description", "")
                    thumbnail = item["snippet"]["thumbnails"]["high"]["url"]
                    published_date = item["snippet"]["publishedAt"]
                    
                    videos.append({
                        "video_id": video_id,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "title": title,
                        "description": description,
                        "thumbnail": thumbnail,
                        "published_date": published_date
                    })
            
            total_results += len(items)
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        
        return videos
    
    except HttpError as e:
        st.error(f"API Error for channel {channel_name}: {e}")
        return []

# Fetch video statistics in batches
def get_video_statistics(videos):
    youtube = get_youtube_service()
    
    if not videos:
        return videos
    
    video_ids = [video["video_id"] for video in videos]
    video_dict = {video["video_id"]: video for video in videos}
    
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        
        try:
            request = youtube.videos().list(
                part="statistics",
                id=",".join(chunk)
            )
            response = request.execute()
            
            for item in response.get("items", []):
                video_id = item["id"]
                if video_id in video_dict:
                    stats = item.get("statistics", {})
                    video_dict[video_id]["views"] = int(stats.get("viewCount", 0))
                    video_dict[video_id]["likes"] = int(stats.get("likeCount", 0))
                    video_dict[video_id]["comments"] = int(stats.get("commentCount", 0))
            
        except HttpError as e:
            st.error(f"API Error when fetching statistics: {e}")
    
    return list(video_dict.values())

# Check if data needs refreshing
def needs_refresh(data, max_age_days=7):
    if not data:
        return True
    current_date = datetime.datetime.now()
    for item in data:
        fetch_date_str = item.get("fetch_date")
        if not fetch_date_str:
            return True
        try:
            fetch_date = datetime.datetime.fromisoformat(fetch_date_str)
            age = (current_date - fetch_date).days
            if age > max_age_days:
                return True
        except ValueError:
            return True
    return False

# Recreate database if needed
def recreate_database():
    try:
        if os.path.exists("youtube_data.db"):
            os.remove("youtube_data.db")
            st.success("Database file removed successfully.")
        initialize_db()
        st.success("Database has been recreated with the new schema.")
    except Exception as e:
        st.error(f"Error recreating database: {e}")

# Initialize Database
initialize_db()

# Apply Styling with additional CSS for metrics
st.set_page_config(layout="wide", page_title="YouTube Outlier Detector")
st.markdown("""
<style>
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
    .video-card {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 15px;
        background-color: #f9f9f9;
    }
    .video-title {
        font-weight: bold;
        margin-bottom: 5px;
    }
    .video-stats {
        display: flex;
        justify-content: space-between;
        margin-top: 10px;
    }
    /* Reduce the font size for metric values and labels */
    div[data-testid="stMetricValue"] {
        font-size: 14px !important;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 12px !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("🎥 YouTube Outlier Video Detector")
st.markdown("Find standout videos across channels based on performance metrics")

# Sidebar for controls
with st.sidebar:
    st.header("🔍 Filter Options")
    niche_data = load_niche_channels()
    niches = list(niche_data.keys())
    
    if not niches:
        st.error("No niches found in channels.json. Please check the file.")
        selected_niche = None
    else:
        selected_niche = st.selectbox("Select a Niche", niches)
    
    keyword = st.text_input("🔎 Enter keyword to search within videos")
    
    st.subheader("Advanced Options")
    outlier_threshold = st.slider("Minimum Outlier Score", min_value=0, max_value=20, value=5, step=1)
    
    max_results_per_channel = st.slider("Max Videos per Channel", min_value=10, max_value=200, value=50, step=10)
    
    refresh_days = st.slider("Refresh data older than (days)", min_value=1, max_value=30, value=7, step=1)
    force_refresh = st.checkbox("Force refresh all data")
    
    sort_options = {
        "View Count": "views",
        "Outlier Score": "outlier_score",
        "Likes": "likes",
        "Comments": "comments" 
    }
    
    sort_option = st.selectbox("Sort results by", list(sort_options.keys()))
    
    fetch_button = st.button("🔍 Find Outliers")
    
    st.header("🛠️ Database Maintenance")
    
    if st.button("🗑️ Clear Cache"):
        clear_cache()
        st.success("Cache cleared! All data has been removed from the database.")
    
    if st.button("🔄 Recreate Database"):
        recreate_database()
        st.success("Database has been recreated with the updated schema.")

# Main content area
if fetch_button and selected_niche:
    with st.spinner("Searching for videos..."):
        try:
            db_results = search_db_results(
                niche=selected_niche,
                keyword=keyword, 
                min_outlier_score=outlier_threshold,
                sort_by=sort_options[sort_option],
                niche_data=niche_data
            )
            
            refresh_needed = needs_refresh(db_results, max_age_days=refresh_days) or force_refresh
            
            if db_results and not refresh_needed:
                st.success(f"✅ Found {len(db_results)} videos in the database")
                video_data = db_results
            else:
                st.info("Fetching fresh data from YouTube...")
                niche_channels = niche_data.get(selected_niche, [])
                if not niche_channels:
                    st.warning(f"No channels found for the niche: {selected_niche}")
                    video_data = []
                else:
                    all_videos = []
                    progress_bar = st.progress(0)
                    
                    for i, channel in enumerate(niche_channels):
                        channel_videos = get_channel_videos(
                            channel["channel_id"], 
                            channel["channel_name"],
                            max_results=max_results_per_channel
                        )
                        
                        if channel_videos:
                            progress_bar.progress((i + 0.5) / len(niche_channels))
                            st.text(f"Fetching statistics for {len(channel_videos)} videos...")
                            channel_videos = get_video_statistics(channel_videos)
                            all_videos.extend(channel_videos)
                        
                        progress_bar.progress((i + 1) / len(niche_channels))
                    
                    if all_videos:
                        st.text("Calculating outlier scores...")
                        all_videos = compute_outlier_scores(all_videos, metric="views")
                        save_to_db(all_videos)
                        video_data = [
                            video for video in all_videos
                            if (not keyword or 
                                (keyword.lower() in video["title"].lower() or 
                                 keyword.lower() in video.get("description", "").lower())) and
                            video.get("outlier_score", 0) >= outlier_threshold
                        ]
                        # Remove duplicates if any
                        video_data = list({video["video_id"]: video for video in video_data}.values())
                        video_data = sorted(
                            video_data, 
                            key=lambda x: x.get(sort_options[sort_option], 0), 
                            reverse=True
                        )
                        st.success(f"✅ Found {len(video_data)} videos matching your criteria")
                    else:
                        st.warning("No videos found for the selected niche.")
                        video_data = []
            
            # After obtaining search results, fetch top 50 comments for each video
            if video_data:
                with st.spinner("Fetching top 50 comments for each video..."):
                    for video in video_data:
                        video["top_comments"] = get_video_comments(video["video_id"], max_results=50)
            
            # Display the results
            if video_data:
                df = pd.DataFrame(video_data)
                if 'channel_name' in df.columns and 'title' in df.columns:
                    display_cols = ['channel_name', 'title', 'views', 'likes', 'comments', 'outlier_score']
                    display_names = ['Channel', 'Title', 'Views', 'Likes', 'Comments', 'Outlier Score']
                    for col in display_cols:
                        if col not in df.columns:
                            df[col] = "N/A"
                    df_display = df[display_cols].copy()
                    df_display.columns = display_names
                    
                    st.subheader("Results Overview")
                    st.dataframe(df_display, height=300)
                
                st.subheader("Top Videos")
                top_videos = video_data[:10]
                # Create two columns for displaying videos side by side
                cols = st.columns(2)
                for idx, video in enumerate(top_videos):
                    with cols[idx % 2]:
                        st.image(video.get("thumbnail"), use_container_width=True)
                        st.markdown(f"### {video.get('title', 'No Title')}")
                        st.markdown(f"**Channel:** {video.get('channel_name', 'Unknown')}")
                        stat_cols = st.columns(4)
                        with stat_cols[0]:
                            st.metric("Views", f"{video.get('views', 0):,}")
                        with stat_cols[1]:
                            st.metric("Likes", f"{video.get('likes', 0):,}")
                        with stat_cols[2]:
                            st.metric("Comments", f"{video.get('comments', 0):,}")
                        with stat_cols[3]:
                            st.metric("Outlier Score", f"{video.get('outlier_score', 0):.2f}")
                        video_url = f"https://www.youtube.com/watch?v={video.get('video_id', '')}"
                        st.markdown(f"[Watch Video]({video_url})")
                        with st.expander("Show Top 50 Comments & Sentiment Analysis"):
                            comments = video.get("top_comments", [])
                            if comments:
                                st.markdown("#### Comments")
                                for comment in comments:
                                    st.markdown(f"- {comment}")
                                st.markdown("#### Sentiment Analysis")
                                sentiments = analyze_sentiment_for_comments(comments)
                                st.markdown("**Positive Comments:**")
                                if sentiments.get("positive"):
                                    for comment in sentiments["positive"]:
                                        st.markdown(f"- {comment}")
                                else:
                                    st.markdown("None")
                                st.markdown("**Neutral Comments:**")
                                if sentiments.get("neutral"):
                                    for comment in sentiments["neutral"]:
                                        st.markdown(f"- {comment}")
                                else:
                                    st.markdown("None")
                                st.markdown("**Negative Comments:**")
                                if sentiments.get("negative"):
                                    for comment in sentiments["negative"]:
                                        st.markdown(f"- {comment}")
                                else:
                                    st.markdown("None")
                            else:
                                st.markdown("No comments available.")
                        st.markdown("---")
            
            else:
                st.warning("No videos found matching your criteria. Try adjusting your filters.")
                
        except Exception as e:
            st.error(f"An error occurred: {e}")
            import traceback
            st.error(traceback.format_exc())
elif fetch_button and not selected_niche:
    st.warning("Please select a niche first!")
else:
    st.info("👈 Select filters and click 'Find Outliers' to search for videos")
    st.markdown("""
    ## How to use this app
    
    1. **Select a niche** from the dropdown menu
    2. Optionally **enter a keyword** to search within video titles and descriptions
    3. Adjust the **minimum outlier score** to filter for truly exceptional videos
    4. Choose how to **sort** your results
    5. Click the **Find Outliers** button to see the results
    
    ## What is an outlier score?
    
    The outlier score measures how much a video deviates from the typical performance of videos in the same niche:
    
    - **Score > 5**: The video is performing better than most others
    - **Score > 10**: The video is a significant outlier (very successful)
    - **Score > 15**: The video is an extreme outlier (viral potential)
    
    The score is calculated using a statistical method called Modified Z-Score based on median absolute deviation.
    """)
