import os
import logging
from datetime import datetime
import h5py
import pandas as pd
import praw
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from afinn import Afinn

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'reddit_scraping.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Data directory
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id='RGc5njLSJJ62mAeGmtksRg',
    client_secret='i80yxadLcNSO-ZrW1rYAJceyKayqWw',
    user_agent='nlp-xdefiant by u/PsybientPanda'
)

def get_reddit_data(subreddit_name, limit=100):
    """Fetch data from a specified subreddit."""
    subreddit = reddit.subreddit(subreddit_name)
    posts = []
    for submission in subreddit.hot(limit=limit):
        if submission.is_self:  # Filter out non-text posts
            posts.append([submission.title, submission.selftext, submission.score, submission.id,
                          submission.subreddit, submission.url, submission.num_comments, submission.created,
                          submission.author])
    posts_df = pd.DataFrame(posts, columns=['title', 'body', 'score', 'id', 'subreddit', 'url',
                                            'num_comments', 'created', 'author'])
    return posts_df

def get_comments(post_id):
    """Fetch comments for a given post."""
    submission = reddit.submission(id=post_id)
    submission.comments.replace_more(limit=None)
    comments = []
    for comment in submission.comments.list():
        if hasattr(comment, 'body'):
            comments.append([comment.body, comment.score, comment.id, comment.created, comment.author])
    comments_df = pd.DataFrame(comments, columns=['body', 'score', 'id', 'created', 'author'])
    return comments_df

def perform_sentiment_analysis(text):
    """Perform sentiment analysis using TextBlob, VADER, and AFINN."""
    # Ensure text is a string
    if not isinstance(text, str):
        logging.debug(f"Non-string object detected: {text} (type: {type(text)})")
        text = str(text)
        
    # TextBlob
    blob = TextBlob(text)
    textblob_polarity, textblob_subjectivity = blob.sentiment.polarity, blob.sentiment.subjectivity

    # VADER
    vader_analyzer = SentimentIntensityAnalyzer()
    vader_scores = vader_analyzer.polarity_scores(text)

    # AFINN
    afinn = Afinn()
    afinn_score = afinn.score(text)

    return {
        'textblob_polarity': textblob_polarity,
        'textblob_subjectivity': textblob_subjectivity,
        'vader_neg': vader_scores['neg'],
        'vader_neu': vader_scores['neu'],
        'vader_pos': vader_scores['pos'],
        'vader_compound': vader_scores['compound'],
        'afinn_score': afinn_score
    }

def main():
    subreddit_name = 'XDefiant'
    limit = 100
    try:
        # Fetch posts
        posts_df = get_reddit_data(subreddit_name, limit)
        logging.info(f'Successfully fetched {len(posts_df)} posts from {subreddit_name}')
        
        all_comments = []
        for post_id in posts_df['id']:
            comments_df = get_comments(post_id)
            logging.info(f'Successfully fetched {len(comments_df)} comments from post {post_id}')
            all_comments.append(comments_df)
        
        comments_df = pd.concat(all_comments, ignore_index=True)
        
        # Perform sentiment analysis on posts
        post_sentiments = posts_df['body'].apply(lambda x: perform_sentiment_analysis(str(x)))
        post_sentiment_df = pd.DataFrame(post_sentiments.tolist())
        posts_result_df = pd.concat([posts_df, post_sentiment_df], axis=1)

        # Perform sentiment analysis on comments
        comment_sentiments = comments_df['body'].apply(lambda x: perform_sentiment_analysis(str(x)))
        comment_sentiment_df = pd.DataFrame(comment_sentiments.tolist())
        comments_result_df = pd.concat([comments_df, comment_sentiment_df], axis=1)

        # Convert all columns to string type
        posts_result_df = posts_result_df.astype(str)
        comments_result_df = comments_result_df.astype(str)

        # Save to HDF5
        hdf5_path = os.path.join(data_dir, 'data.h5')
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        with h5py.File(hdf5_path, 'a') as hdf:
            post_grp = hdf.create_group(f"reddit_{subreddit_name}_posts/{timestamp}")
            post_ds = post_grp.create_dataset('data', data=posts_result_df.to_numpy(), dtype=h5py.string_dtype(encoding='utf-8'))
            post_ds.attrs['columns'] = list(posts_result_df.columns)
            
            comment_grp = hdf.create_group(f"reddit_{subreddit_name}_comments/{timestamp}")
            comment_ds = comment_grp.create_dataset('data', data=comments_result_df.to_numpy(), dtype=h5py.string_dtype(encoding='utf-8'))
            comment_ds.attrs['columns'] = list(comments_result_df.columns)

        logging.info(f"Saved data to HDF5 groups: reddit_{subreddit_name}_posts/{timestamp} and reddit_{subreddit_name}_comments/{timestamp}")
    except Exception as e:
        logging.error(f'Error occurred: {e}')

if __name__ == "__main__":
    main()
