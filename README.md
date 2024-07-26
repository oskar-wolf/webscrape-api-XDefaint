# Web Scraping and Data Visualization Project

## Project Overview
This project demonstrates web scraping skills to collect numeric information from several web pages, extract the relevant numeric information, and save the results in a format that allows for time series considerations. The collected data is then visualized in a dashboard to provide insights into the temporal development of the values.

## Prerequisites
- Python 3.x
- Required Python packages are listed in `requirements.txt`. Install them using:
  ```bash
  pip install -r requirements.txt

## Directory Structure
  ```bash
webscrape_api_project/
│
├── data/
│   ├── data.h5
│   ├── preprocessed_data.h5
│   └── webpages.csv
│
├── logs/
│   ├── main.log
│   ├── preprocess_finance.log
│   ├── preprocess_ign_ratings.log
│   ├── preprocess_leaderboards.log
│   ├── preprocess_player_counts.log
│   ├── preprocess_reddit_comments.log
│   ├── preprocess_reddit_posts.log
│   ├── preprocess_web_search.log
│   ├── preprocess_youtube_search.log
│   ├── reddit_scraping.log
│   └── scraping.log
│
├── scripts/
│   ├── main.py
│   ├── preprocess_finance.py
│   ├── preprocess_ign_ratings.py
│   ├── preprocess_leaderboard.py
│   ├── preprocess_player_count.py
│   ├── preprocess_reddit_comments.py
│   ├── preprocess_reddit_posts.py
│   ├── preprocess_web_search.py
│   ├── preprocess_youtube_search.py
│   ├── scrape_finance.py
│   ├── scrape_google_trends.py
│   ├── scrape_ign_rating.py
│   ├── scrape_player_count.py
│   ├── scrape_reddit.py
│   └── scrape_tracker.py
│
└── visualizations/
    ├── visualize_data.py
    └── README.md
