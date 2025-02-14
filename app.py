import streamlit as st
from googleapiclient.discovery import build
import pandas as pd

# API Key (Replace with your own API key)
API_KEY = "AIzaSyA8Sq9m4VQAen1moeVw9kkaZw575z3rQY0"

def get_channel_stats(channel_id):
    """Fetch channel statistics including subscriber count, total views, and video count."""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()
    if "items" in response:
        channel_data = response["items"][0]
        stats = channel_data["statistics"]
        return {
            "title": channel_data["snippet"]["title"],
            "thumbnail": channel_data["snippet"]["thumbnails"].get("default", {}).get("url", ""),
            "subscribers": int(stats.get("subscriberCount", 0)),
            "views": int(stats.get("viewCount", 0)),
            "videos": int(stats.get("videoCount", 0))
        }
    return None

def get_video_stats(video_id):
    """Fetch video statistics and metadata."""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    )
    response = request.execute()
    if "items" in response:
        video_data = response["items"][0]
        stats = video_data["statistics"]
        duration = video_data["contentDetails"]["duration"]
        is_short = "PT" in duration and "M" not in duration  # Detecting if it's a short
        return {
            "title": video_data["snippet"]["title"],
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
            "publish_date": video_data["snippet"].get("publishedAt", ""),
            "thumbnail": video_data["snippet"]["thumbnails"].get("medium", {}).get("url", ""),
            "is_short": is_short,
            "video_id": video_id
        }
    return None

def get_video_comments(video_id, max_results=10):
    """Fetch comments for a given video."""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=max_results
    )
    response = request.execute()
    comments = []
    if "items" in response:
        for item in response["items"]:
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "author": comment["authorDisplayName"],
                "comment": comment["textDisplay"],
                "likes": comment["likeCount"],
                "published": comment["publishedAt"]
            })
    return comments

def get_top_videos(channel_id, max_results=10):
    """Fetch top performing videos based on views."""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=max_results,
        order="viewCount",
        type="video"
    )
    response = request.execute()
    videos = []
    for item in response["items"]:
        video_id = item["id"]["videoId"]
        video_stats = get_video_stats(video_id)
        if video_stats:
            videos.append(video_stats)
    return videos

def main():
    if "page" not in st.session_state:
        st.session_state.page = "home"

    if st.session_state.page == "home":
        st.title("YouTube Competitive Analysis & Outlier Detection")
        st.markdown("---")
        channel_url = st.text_input("Enter YouTube Channel ID or Video URL:")

        if st.button("Analyze Channel"):
            if "channel" in channel_url:
                channel_id = channel_url.split("/")[-1]
            elif "watch?v=" in channel_url:
                video_id = channel_url.split("=")[-1]
                video_stats = get_video_stats(video_id)
                if video_stats:
                    st.session_state.video_data = video_stats
                    st.session_state.page = "video_detail"
                    st.experimental_rerun()
            else:
                st.error("Invalid URL. Please enter a valid channel ID or video URL.")
                return

            channel_stats = get_channel_stats(channel_id)
            if channel_stats:
                st.header(channel_stats["title"])
                st.image(channel_stats["thumbnail"], width=100)
                st.write(f"Subscribers: {channel_stats['subscribers']:,}")
                st.write(f"Total Views: {channel_stats['views']:,}")
                st.write(f"Total Videos: {channel_stats['videos']:,}")

                top_videos = get_top_videos(channel_id)
                if top_videos:
                    toggle = st.radio("Select Video Type", ("Longform Videos", "Shorts"))
                    df_videos = pd.DataFrame(top_videos)
                    if toggle == "Longform Videos":
                        df_videos = df_videos[df_videos["is_short"] == False]
                    else:
                        df_videos = df_videos[df_videos["is_short"] == True]

                    if not df_videos.empty:
                        st.dataframe(df_videos[["title", "views", "likes", "comments"]].style.format({
                            "views": "{:,}", "likes": "{:,}", "comments": "{:,}"}))
                    else:
                        st.write("No videos found for the selected category.")
            else:
                st.error("Failed to fetch channel details. Please check the ID.")

    elif st.session_state.page == "video_detail":
        video_data = st.session_state.video_data
        st.title(video_data["title"])
        st.image(video_data["thumbnail"], width=300)
        st.write(f"Views: {video_data['views']:,}")
        st.write(f"Likes: {video_data['likes']:,}")
        st.write(f"Comments: {video_data['comments']:,}")

        st.markdown("### Top Comments")
        comments = get_video_comments(video_data["video_id"])
        if comments:
            df_comments = pd.DataFrame(comments)
            st.dataframe(df_comments[["author", "comment", "likes", "published"]])
        else:
            st.write("No comments found.")

        if st.button("Back to Home"):
            st.session_state.page = "home"
            st.experimental_rerun()

if __name__ == "__main__":
    main()
