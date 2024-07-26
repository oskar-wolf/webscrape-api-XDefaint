import time
import os
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import h5py
import pandas as pd  

log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir,'scraping.log'),level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

url = 'https://www.playerauctions.com/player-count/xdefiant/'

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
    response = requests.get(url,headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.content,'html.parser')

    current_player_count_element = soup.select_one("#__next > div > main > div > div > div.playercount_container___PSMv > section:nth-child(5) > div.playercount_chartTemp__YQMld > div.playercount_latestCount__yDUAq > div.playercount_latestCountYesterday__8OZXQ > div:nth-child(1) > span")
    current_player_count = current_player_count_element.get_text(strip=True) if current_player_count_element else "N/A"
    logging.info(f'Current Playes: {current_player_count}')

  # Extract the comparison to the previous day
    comparison_element = soup.select_one('#__next > div > main > div > div > div.playercount_container___PSMv > section:nth-child(5) > div.playercount_chartTemp__YQMld > div.playercount_latestCount__yDUAq > div.playercount_latestCountYesterday__8OZXQ > div.playercount_latestCountYesterdayPercent__YM0LP > div:nth-child(1) > span')
    previous_day_change = 'N/A'
    if comparison_element:
        comparison_text = comparison_element.get_text(strip=True)
        # Extract the percentage value within parentheses
        if "(" in comparison_text and "%" in comparison_text:
            previous_day_change = comparison_text.split("(")[-1].split("%")[0].strip() + "%"

    logging.info(f"Comparison to Previous Day: {previous_day_change}")

   # Create a dictionary with the data
    player_data = {
        'Timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'Current Players': [current_player_count],
        'Previous Day Change': [previous_day_change]
    }

    df = pd.DataFrame(player_data)

    hdf5_path = os.path.join(data_dir,'data.h5')
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    with h5py.File(hdf5_path,'a') as hdf:
        grp = hdf.create_group(f"player_counts/{timestamp}")
        ds = grp.create_dataset('data',data=df.to_numpy(),dtype=h5py.string_dtype(encoding='utf-8'))
        ds.attrs['columns'] = list(df.columns)
        logging.info(f"Saved data to HDF5 group: player_counts/{timestamp}")
except Exception as e:
    logging.error(f"Failed to scrape player count data: {e}")

