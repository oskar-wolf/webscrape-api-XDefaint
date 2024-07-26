import requests
from bs4 import BeautifulSoup
import h5py
import numpy as np
from datetime import datetime
import os
import logging

# Get the absolute path to the logs directory
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))

# Create logs directory if it does not exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
logging.basicConfig(filename=os.path.join(log_dir, 'scraping.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Get the absolute path to the data directory
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

# Create data directory if it does not exist
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Function to fetch the leaderboard HTML content from the given URL
def fetch_leaderboard(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Send a GET request to the URL
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error if the request was unsuccessful
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

# Function to parse the leaderboard data from the BeautifulSoup object
def parse_leaderboard(soup):
    leaderboard_data = []

    # Find the container that holds the leaderboard table
    table_container = soup.find('div', class_='trn-table__container')
    if table_container:
        # Find the leaderboard table within the container
        table = table_container.find('table', class_='trn-table')
        if table:
            # Find all the rows in the table
            rows = table.find_all('tr')
            # Iterate through each row, skipping the header row
            for row in rows[1:]:
                columns = row.find_all('td')
                rank = columns[0].text.strip()  # Extract rank
                player = columns[1].text.strip()  # Extract player name
                
                flag = 'No Flag Found'
                # Find the flag icon within the player column
                flag_wrapper = columns[1].find('div', class_='flag-icon-wrapper')
                if flag_wrapper:
                    img_tag = flag_wrapper.find('img')
                    if img_tag and 'src' in img_tag.attrs:
                        flag = img_tag['src']  # Extract flag URL

                kills = columns[2].text.strip().replace(',', '')  # Extract kills and remove commas
                matches_played = columns[3].text.strip().replace(',', '')  # Extract matches played and remove commas
                
                # Append the parsed data to the leaderboard data list
                leaderboard_data.append([str(rank), str(player), str(flag), str(kills), str(matches_played)])
            return leaderboard_data
        else:
            logging.error("Leaderboard table not found")
            return []
    else:
        logging.error("Leaderboard table container not found")
        return []

# Function to save the parsed data to an HDF5 file
def save_to_hdf5(data, flag_images, timestamp, filename='data.h5'):
    hdf5_file_path = os.path.join(data_dir, filename)  # Define the file path
    group_name = f"leaderboards/{timestamp}"  # Create a group name with timestamp
    
    with h5py.File(hdf5_file_path, 'a') as h5f:
        # Create a new group for the current timestamp
        group = h5f.create_group(group_name)
        dtype = h5py.special_dtype(vlen=str)
        # Create a dataset within the group to store the leaderboard data
        group.create_dataset('leaderboard', data=np.array(data, dtype=dtype))
        
        # Save flag images
        for url, image in flag_images.items():
            group.create_dataset(f"flags/{url}", data=np.void(image))

def main():
    url = 'https://tracker.gg/xdefiant/leaderboards/stats/all/Kills?page=1'  # Replace with the actual URL
    try:
        # Fetch the leaderboard data
        soup = fetch_leaderboard(url)
        leaderboard_data = parse_leaderboard(soup)
        
        if leaderboard_data:
            # Generate a timestamp for the current run
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            flag_images = {}
            
            # Download flag images and store them in a dictionary
            for entry in leaderboard_data:
                flag_url = entry[2]
                if flag_url != 'No Flag Found' and flag_url not in flag_images:
                    try:
                        flag_response = requests.get(flag_url)
                        flag_response.raise_for_status()
                        flag_images[flag_url] = flag_response.content
                    except (requests.RequestException, UnidentifiedImageError):
                        logging.error(f"Failed to identify image from {flag_url}")
            
            # Save the leaderboard data and flag images to an HDF5 file
            save_to_hdf5(leaderboard_data, flag_images, timestamp)
            logging.info(f"Data and images saved to {os.path.join(data_dir, 'leaderboard_data.h5')} in group leaderboards/{timestamp}")
        else:
            logging.warning("No leaderboard data found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
