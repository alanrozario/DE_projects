from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables from the .env file
load_dotenv()

# Access the API key from environment variables
youtube_api_key = os.getenv("YOUTUBE_API_KEY")

# Initialize YouTube API client
youtube = build("youtube", "v3", developerKey=youtube_api_key)

def search_youtube(query, max_results=10):
    """
    Search YouTube for videos matching the query.

    Args:
        query (str): The search query.
        max_results (int): Number of video results to return.

    Returns:
        list: A list of video IDs for the first `max_results` videos.
    """
    try:
        # Search for videos using the YouTube Data API
        search_response = youtube.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=max_results
        ).execute()

        # Extract video IDs from the search response
        video_ids = [
            item["id"]["videoId"] for item in search_response.get("items", [])
        ]

        return video_ids

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def get_video_comments(video_ids, max_results=100):
    """
    Fetch comments for a given YouTube video.

    Args:
        video_id (str): The ID of the YouTube video.
        max_results (int): Number of comments to fetch.

    Returns:
        DataFrame: A DataFrame with the video comments and metadata.
    """
    
    for video_id in video_ids:
        try:
            # Request comments for the video
            comments_request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_results,
                textFormat="plainText",
            )
            comments_response = comments_request.execute()

            # Extract relevant attributes
            attributes_container = []
            for item in comments_response.get("items", []):
                comment = item["snippet"]["topLevelComment"]["snippet"]
                attributes_container.append({
                    "User": comment["authorDisplayName"],
                    "Date_Created": comment["publishedAt"],
                    "Likes": comment["likeCount"],
                    "Comment": comment["textDisplay"],
                })

            # Create a DataFrame
            comments_df = pd.DataFrame(attributes_container)

            return comments_df

        except Exception as e:
            print(f"Failed to fetch comments: {e}")
            return pd.DataFrame()

# Product to review
product = 'iphone 15'
search_query = f"Product Review {product}"
video_ids = search_youtube(search_query)

video_ids = search_youtube(search_query, max_results=10)  # Example: 'dQw4w9WgXcQ'
comments_df = get_video_comments(video_ids, max_results=100)

# Print the DataFrame
print(comments_df)

# Save the DataFrame to a CSV file (optional)
comments_df.to_csv("youtube_comments.csv", index=False)