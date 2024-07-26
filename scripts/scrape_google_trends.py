import time
import os
import random
import logging
import glob
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import h5py

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

# Path to geckodriver
geckodriver_path = 'C:\\webdrivers\\geckodriver.exe'

# Path to Firefox binary
firefox_binary_path = 'C:\\Program Files\\Mozilla Firefox\\firefox.exe'

# Firefox options to avoid being detected as a bot
firefox_options = Options()
firefox_options.headless = False  # Run in headless mode
firefox_options.binary_location = firefox_binary_path  # Specify the path to the Firefox binary
firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
firefox_options.set_preference("dom.webdriver.enabled", False)
firefox_options.set_preference('useAutomationExtension', False)
firefox_options.set_preference("browser.download.folderList", 2)
firefox_options.set_preference("browser.download.manager.showWhenStarting", False)
firefox_options.set_preference("browser.download.dir", data_dir)
firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

service = Service(executable_path=geckodriver_path)
driver = webdriver.Firefox(service=service, options=firefox_options)

# Load a general Google Trends page first
driver.get('https://trends.google.com/trends/')
logging.info("Loaded general Google Trends page")
time.sleep(random.uniform(10, 20))  # Random delay to mimic human behavior

def save_to_hdf5(csv_file, group_name):
    df = pd.read_csv(csv_file, sep=',')
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    hdf5_path = os.path.join(data_dir, 'data.h5')
    
    with h5py.File(hdf5_path, 'a') as hdf:
        grp = hdf.create_group(f"{group_name}/{timestamp}")
        for col in df.columns:
            grp.create_dataset(col, data=df[col].values, dtype=h5py.string_dtype(encoding='utf-8'))
        logging.info(f"Saved data to HDF5 group: {group_name}/{timestamp}")

# Function to download data and save to HDF5
def download_trends_data(url, filename, group_name):
    success = False
    attempts = 0
    while not success and attempts < 5:  # Retry up to 5 times
        if os.path.exists(os.path.join(data_dir, filename)):
            logging.info(f"File {filename} already exists. Skipping download.")
            break
        try:
            driver.get(url)
            logging.info(f"Loaded URL: {url}")
            
            time.sleep(random.uniform(5, 10))  # Random delay to mimic human behavior
            
            # Wait for the export button to be clickable
            export_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.widget-actions-item.export'))
            )
            
            # Click the export button
            export_button.click()
            logging.info(f"Clicked export button for URL: {url}")
            
            # Wait for the CSV download to complete
            time.sleep(random.uniform(10, 20))  # Random delay to ensure download completes

            # Rename the downloaded file
            latest_file = max(glob.glob(os.path.join(data_dir, '*.csv')), key=os.path.getctime)
            new_filename = os.path.join(data_dir, filename)
            os.rename(latest_file, new_filename)
            logging.info(f"Renamed downloaded file to {new_filename}")

            # Save the data to HDF5
            save_to_hdf5(new_filename, group_name)
            success = True
        except Exception as e:
            logging.error(f"Failed to download trends data from {url}: {e}")
            attempts += 1
            time.sleep(random.uniform(10, 20))  # Random delay before retrying

# URLs for Google Trends
web_search_url = 'https://trends.google.com/trends/explore?date=now%201-d&q=%2Fg%2F11rscfj9f0&hl=en-ZA'
youtube_search_url = 'https://trends.google.com/trends/explore?date=now%201-d&gprop=youtube&q=%2Fg%2F11rscfj9f0&hl=en-ZA'

# Download web search data
web_success = False
while not web_success:
    try:
        download_trends_data(web_search_url, 'web_search.csv', 'web_search')
        web_success = True
    except Exception as e:
        logging.error(f"Retrying web search download: {e}")
        time.sleep(random.uniform(10, 20))  # Random delay before retrying

# Download YouTube search data
youtube_success = False
while not youtube_success:
    try:
        download_trends_data(youtube_search_url, 'youtube_search.csv', 'youtube_search')
        youtube_success = True
    except Exception as e:
        logging.error(f"Retrying YouTube search download: {e}")
        time.sleep(random.uniform(10, 20))  # Random delay before retrying

# Remove the CSV files after processing
os.remove(os.path.join(data_dir, 'web_search.csv'))
os.remove(os.path.join(data_dir, 'youtube_search.csv'))

driver.quit()
