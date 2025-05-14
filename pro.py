import tweepy
from datetime import datetime
import pandas as pd
import json
import s3fs

# Your Twitter API credentials
access_key = 'iAGkhZoiamJKSMLb1SfenYewY'
secret_access_key = 'hAzNCydq0s49iXSyAkjJnBF3VufNVqC4zTwSJafYUTGolM2Zhc'
consumer_key = '1916769825558568960-9g11HFkC6RB9vli4o0FFV8Eg94soVm'
consumer_secret_key = 'LzhEtDhSRh3GDgyMQGBJSW2M7MwsXsqM0xJvNMKIRFNjq'
bearer_token='AAAAAAAAAAAAAAAAAAAAAHz11AEAAAAANp1xsiDCSBedLEl7er1Fe1c%2FoFI%3DSuzsoN5dqbTiQa2F9xb9HOQGa21exSA5AIug1knLLjOwnpIx8P'

def run_twitter_etl():

    # Replace with your actual Bearer Token from X Developer Portal
    client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAHz11AEAAAAANp1xsiDCSBedLEl7er1Fe1c%2FoFI%3DSuzsoN5dqbTiQa2F9xb9HOQGa21exSA5AIug1knLLjOwnpIx8P')

    # Get user ID from username
    user = client.get_user(username="realdonaldtrump")
    user_id = user.data.id

    # Step 2: Get tweets with necessary fields
    tweets = client.get_users_tweets(
        id=user_id,
        max_results=100,
        tweet_fields=["created_at", "public_metrics","text"]
    )

    # Step 3: Process and save
    lst = []
    if tweets.data:
        for tweet in tweets.data:
            refined_tweet = {
                "user": "AlJazeera",
                "text": tweet.text,
                "favorite_count": tweet.public_metrics["like_count"],
                "retweet_count": tweet.public_metrics["retweet_count"],
                "created_at": tweet.created_at
            }
            lst.append(refined_tweet)

        df = pd.DataFrame(lst)
        df.to_csv("s3://twitter-etl-airflow-project/refined_tweets.csv", index=False)
        print("Saved to refined_tweets.csv")
    else:
        print("No tweets found.")
