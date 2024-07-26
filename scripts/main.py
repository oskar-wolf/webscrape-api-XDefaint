# import logging
# import os

# # Configure logging
# log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
# if not os.path.exists(log_dir):
#     os.makedirs(log_dir)

# logging.basicConfig(filename=os.path.join(log_dir, 'main.log'), level=logging.INFO,
#                     format='%(asctime)s:%(levelname)s:%(message)s')

# # Importing and running each script
# def run_script(script_path):
#     try:
#         logging.info(f"Running {script_path}...")
#         os.system(f"python {script_path}")
#         logging.info(f"Completed {script_path}")
#     except Exception as e:
#         logging.error(f"Failed to run {script_path}: {e}")

# if __name__ == "__main__":
#     base_dir = os.path.dirname(os.path.abspath(__file__))
#     scripts_dir = base_dir  # Directly use base_dir since the scripts are in the same directory

#     scrape_scripts = [
#         'scrape_finance.py',
#         'scrape_google_trends.py',
#         'scrape_ign_rating.py',
#         'scrape_player_count.py',
#         'scrape_reddit.py',
#         'scrape_tracker.py'
#     ]

#     preprocess_scripts = [
#         'preprocess_finance.py',
#         'preprocess_ign_ratings.py',
#         'preprocess_leaderboard.py',
#         'preprocess_player_count.py',
#         'preprocess_reddit_comments.py',
#         'preprocess_reddit_posts.py',
#         'preprocess_web_search.py',
#         'preprocess_youtube_search.py'
#     ]

#     # Run scraping scripts
#     for script in scrape_scripts:
#         script_path = os.path.join(scripts_dir, script)
#         run_script(script_path)
    
#     # Run preprocessing scripts
#     for script in preprocess_scripts:
#         script_path = os.path.join(scripts_dir, script)
#         run_script(script_path)

import logging
import os

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(filename=os.path.join(log_dir, 'main.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Importing and running each script
def run_script(script_path):
    try:
        logging.info(f"Running {script_path}...")
        os.system(f"python {script_path}")
        logging.info(f"Completed {script_path}")
    except Exception as e:
        logging.error(f"Failed to run {script_path}: {e}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_dir = base_dir  # Directly use base_dir since the scripts are in the same directory
    visualizations_dir = os.path.join(base_dir, '..', 'visualizations')  # Specify the directory containing the visualization script

    scrape_scripts = [
        'scrape_finance.py',
        'scrape_google_trends.py',
        'scrape_ign_rating.py',
        'scrape_player_count.py',
        'scrape_reddit.py',
        'scrape_tracker.py'
    ]

    preprocess_scripts = [
        'preprocess_finance.py',
        'preprocess_ign_ratings.py',
        'preprocess_leaderboard.py',
        'preprocess_player_count.py',
        'preprocess_reddit_comments.py',
        'preprocess_reddit_posts.py',
        'preprocess_web_search.py',
        'preprocess_youtube_search.py'
    ]

    # Run scraping scripts
    for script in scrape_scripts:
        script_path = os.path.join(scripts_dir, script)
        run_script(script_path)
    
    # Run preprocessing scripts
    for script in preprocess_scripts:
        script_path = os.path.join(scripts_dir, script)
        run_script(script_path)
    
    # Run visualization script
    visualization_script = 'visualize_data.py'
    visualization_script_path = os.path.join(visualizations_dir, visualization_script)
    run_script(visualization_script_path)
