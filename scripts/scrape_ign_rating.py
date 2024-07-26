import time
import os 
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import h5py
import pandas as pd  

# Set up the log directory path
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))

# Create the log directory if it doesn't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging settings
logging.basicConfig(filename=os.path.join(log_dir, 'scraping.log'), level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Set up the data directory path
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

# Create the data directory if it doesn't exist
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# URL of the page to scrape
url = 'https://www.ign.com/games/xdefiant'

# HTTP headers to mimic a real browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'DNT': '1',  # Do Not Track Request Header
}

try:
    # Make a request to the URL
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Parse the content of the request with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract the IGN rating
    ign_rating_element = soup.select_one('span.hexagon-content-wrapper')
    ign_rating = ign_rating_element.get_text(strip=True) if ign_rating_element else 'N/A'
    logging.info(f"IGN Rating: {ign_rating}")

    # Extract the user rating
    user_rating = 'N/A'
    user_rating_element = soup.select_one("#main-content > div > div.jsx-3636063303.wave-header.object-page-heading.collapse > section > div > div.stack.jsx-1500469411.object-header > div.stack.jsx-774472442.meta-items.jsx-4085963266.alt.jsx-4284520940.ur-analytics-block > a:nth-child(3) > div.stack.jsx-2736506000.score-block.high.small > h3")
    if user_rating_element:
        user_rating = user_rating_element.get_text(strip=True)
    logging.info(f"User Rating: {user_rating}")

    # Create a DataFrame with the scraped data
    ratings_data = {
        'Timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'IGN Rating': [ign_rating],
        'User Rating': [user_rating]
    }
    df = pd.DataFrame(ratings_data)

    # Path to the HDF5 file
    hdf5_path = os.path.join(data_dir, 'data.h5')
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Save the DataFrame to the HDF5 file
    with h5py.File(hdf5_path, 'a') as hdf:
        grp = hdf.create_group(f"ign_ratings/{timestamp}")
        ds = grp.create_dataset('ratings', data=df.to_numpy(), dtype=h5py.string_dtype(encoding='utf-8'))
        ds.attrs['columns'] = list(df.columns)
        logging.info(f"Saved data to HDF5 group: ign_ratings/{timestamp}")

except Exception as e:
    # Log any exceptions that occur during the scraping process
    logging.error(f'Failed to scrape IGN Ratings: {e}')
