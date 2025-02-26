import streamlit as st
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
from scipy.stats import median_abs_deviation
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load API Keys from Streamlit Secrets
YOUTUBE_API_KEY = st.secrets["YOUTUBE_API_KEY"]

# Initialize YouTube API
def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# Improved Database Schema
def initialize_db():
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
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
    conn.commit()
    conn.close()

# Clear Cache (Delete all records from the database)
def clear_cache():
    conn = sqlite3.connect("youtube_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM search_results")  # Deletes all stored results
    conn.commit()
    conn.close()

# Fixed database search function
def search_db_results(niche=None, keyword=None, min_outlier_score=None, sort_by="views", niche_data=None):
    conn = sqlite3.connect("youtube_data.db")
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    cursor = conn.cursor()
    
    query_parts = ["""
        SELECT video_id, channel_id, channel_name, title, description, thumbnail, 
               published_date, fetch_date, views, likes, comments, outlier_score 
        FROM search_results
    """]
    
    params = []
    where_conditions = []
    
    # Filter by channel_ids in the selected niche
    if niche and niche_data and niche in niche_data:
        channel_ids = [channel["channel_id"] for channel in niche_data[niche]]
        if channel_ids:
            # Fixed: Use a different approach for IN clause
            channel_placeholders = ",".join(["?"] * len(channel_ids))
            where_conditions.append(f"channel_id IN ({channel_placeholders})")
            params.extend(channel_ids)
    
    # Filter by keyword
    if keyword:
        where_conditions.append("(title LIKE ? OR description LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    
    # Filter by outlier score
    if min_outlier_score is not None:
        where_conditions.append("outlier_score >= ?")
        params.append(min_outlier_score)
    
    # Add WHERE clause if there are conditions
    if where_conditions:
        query_parts.append("WHERE " + " AND ".join(where_conditions))
    
    # Add sorting
    if sort_by:
        query_parts.append(f"ORDER BY {sort_by} DESC")
    
    # Join all parts of the query
    final_query = " ".join(query_parts)
    
    try:
        cursor.execute(final_query, params)
        results = cursor.fetchall()
        
        # Convert to list of dicts
        result_list = []
        for row in results:
            result_list.append(dict(row))
        
        conn.close()
        return result_list
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
    
    # Extract the metric values and video IDs
    values = {}
    for video in videos:
        values[video["video_id"]] = video.get(metric, 0)
    
    # Need at least 2 items for meaningful calculation
    if len(values) < 2:
        for video in videos:
            video["outlier_score"] = 0
        return videos
    
    # Calculate median and MAD
    metric_list = list(values.values())
    median_value = np.median(metric_list)
    mad = median_abs_deviation(metric_list)
    
    # Handle case where MAD is 0
    if mad == 0:
        for video in videos:
            video["outlier_score"] = 0
        return videos
    
    # Calculate modified z-scores
    scores = {vid: 0.6745 * (value - median_value) / mad for vid, value in values.items()}
    
    # Update videos with outlier scores
    for video in videos:
        video["outlier_score"] = round(scores.get(video["video_id"], 0), 2)
    
    return videos

# Improved function to fetch all videos from a channel with pagination
def get_channel_videos(channel_id, channel_name, max_results=100):
    youtube = get_youtube_service()
    videos = []
    next_page_token = None
    
    # Track total results to respect max_results
    total_results = 0
    
    try:
        while total_results < max_results:
            # Determine how many results to request in this batch
            batch_size = min(50, max_results - total_results)
            
            # Make the API request
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
            
            # If there's no next page token, we've reached the end
            if not next_page_token:
                break
        
        return videos
    
    except HttpError as e:
        st.error(f"API Error for channel {channel_name}: {e}")
        return []

# Improved function to fetch video statistics in batches
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
                    # Handle missing statistics gracefully
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
    
    # Check if any of the entries has a fetch_date older than max_age_days
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

# Initialize Database
initialize_db()

# Apply Styling
st.set_page_config(layout="wide", page_title="YouTube Outlier Detector")

# Add custom CSS
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
</style>
""", unsafe_allow_html=True)

st.title("🎥 YouTube Outlier Video Detector")
st.markdown("Find standout videos across channels based on performance metrics")

# Sidebar for controls
with st.sidebar:
    st.header("🔍 Filter Options")
    
    # Load channel data
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
    
    # Clear Cache Button
    if st.button("🗑️ Clear Cache"):
        clear_cache()
        st.success("Cache cleared! The database has been reset.")

# Main content area
if fetch_button and selected_niche:
    with st.spinner("Searching for videos..."):
        try:
            # Check if we have cached results for this niche
            db_results = search_db_results(
                niche=selected_niche,
                keyword=keyword, 
                min_outlier_score=outlier_threshold,
                sort_by=sort_options[sort_option],
                niche_data=niche_data
            )
            
            # Check if we need to fetch fresh data
            refresh_needed = needs_refresh(db_results, max_age_days=refresh_days) or force_refresh
            
            if db_results and not refresh_needed:
                st.success(f"✅ Found {len(db_results)} videos in the database")
                video_data = db_results
            else:
                st.info("Fetching fresh data from YouTube...")
                
                # Get the channels for the selected niche
                niche_channels = niche_data.get(selected_niche, [])
                
                if not niche_channels:
                    st.warning(f"No channels found for the niche: {selected_niche}")
                    video_data = []
                else:
                    all_videos = []
                    progress_bar = st.progress(0)
                    
                    # Fetch videos for each channel
                    for i, channel in enumerate(niche_channels):
                        progress_text = f"Fetching videos from {channel['channel_name']}..."
                        st.text(progress_text)
                        
                        channel_videos = get_channel_videos(
                            channel["channel_id"], 
                            channel["channel_name"],
                            max_results=max_results_per_channel
                        )
                        
                        if channel_videos:
                            # Update progress
                            progress_bar.progress((i + 0.5) / len(niche_channels))
                            st.text(f"Fetching statistics for {len(channel_videos)} videos...")
                            
                            # Get statistics for these videos
                            channel_videos = get_video_statistics(channel_videos)
                            
                            # Add to the overall list
                            all_videos.extend(channel_videos)
                        
                        progress_bar.progress((i + 1) / len(niche_channels))
                    
                    # Calculate outlier scores
                    if all_videos:
                        st.text("Calculating outlier scores...")
                        all_videos = compute_outlier_scores(all_videos, metric="views")
                        
                        # Save to database
                        save_to_db(all_videos)
                        
                        # Filter the results based on criteria
                        video_data = [
                            video for video in all_videos
                            if (not keyword or 
                                (keyword.lower() in video["title"].lower() or 
                                keyword.lower() in video.get("description", "").lower())) and
                            video.get("outlier_score", 0) >= outlier_threshold
                        ]
                        
                        # Sort by the selected option
                        video_data = sorted(
                            video_data, 
                            key=lambda x: x.get(sort_options[sort_option], 0), 
                            reverse=True
                        )
                        
                        st.success(f"✅ Found {len(video_data)} videos matching your criteria")
                    else:
                        st.warning("No videos found for the selected niche.")
                        video_data = []
            
            # Display the results
            if video_data:
                # Create dataframe for overview table
                df = pd.DataFrame(video_data)
                
                # Select and rename columns for display
                if 'channel_name' in df.columns and 'title' in df.columns:
                    display_cols = ['channel_name', 'title', 'views', 'likes', 'comments', 'outlier_score']
                    display_names = ['Channel', 'Title', 'Views', 'Likes', 'Comments', 'Outlier Score']
                    
                    # Ensure all necessary columns exist
                    for col in display_cols:
                        if col not in df.columns:
                            df[col] = "N/A"
                    
                    df_display = df[display_cols].copy()
                    df_display.columns = display_names
                    
                    st.subheader("Results Overview")
                    st.dataframe(df_display, height=300)
                
                # Detailed cards for top results
                st.subheader("Top Videos")
                top_videos = video_data[:10]  # Show top 10
                
                for video in top_videos:
                    col1, col2 = st.columns([1, 3])
                    
                    with col1:
                        if video.get("thumbnail"):
                            st.image(video.get("thumbnail"), use_column_width=True)
                    
                    with col2:
                        st.markdown(f"### {video.get('title', 'No Title')}")
                        st.markdown(f"**Channel:** {video.get('channel_name', 'Unknown')}")
                        
                        # Create three columns for stats
                        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                        with stat_col1:
                            st.metric("Views", f"{video.get('views', 0):,}")
                        with stat_col2:
                            st.metric("Likes", f"{video.get('likes', 0):,}")
                        with stat_col3:
                            st.metric("Comments", f"{video.get('comments', 0):,}")
                        with stat_col4:
                            st.metric("Outlier Score", f"{video.get('outlier_score', 0):.2f}")
                        
                        # Add a link to the video
                        video_url = f"https://www.youtube.com/watch?v={video.get('video_id', '')}"
                        st.markdown(f"[Watch Video]({video_url})")
                    
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
    
    # Display some instructions
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
