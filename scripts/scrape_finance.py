import requests
import h5py
import numpy as np
from datetime import datetime
import os
import logging
import yfinance as yf

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

def fetch_stock_info(ticker):
    # Fetch historical stock data for the given ticker using yfinance.
    # ticker (str): Stock ticker symbol.
    # Returns: DataFrame: Historical stock data with 1-minute intervals for the last trading day.
    
    stock = yf.Ticker(ticker)  # Create a Ticker object using yfinance
    hist = stock.history(period='1d', interval='1m')  # Fetch historical data for the last trading day with 1-minute intervals
    return hist

def parse_stock_info(hist):
    # Parse the historical stock data into a structured format.
    # hist (DataFrame): Historical stock data.
    # Returns: numpy.ndarray: Parsed stock data with timestamp, open, high, low, close, and volume.
    
    # Define a structured numpy array to store the parsed stock data
    stock_data = np.empty(len(hist), dtype=[('timestamp', 'S19'), ('open', 'f4'), 
                                             ('high', 'f4'), ('low', 'f4'), 
                                             ('close', 'f4'), ('volume', 'f4')])

    # Iterate over the historical stock data and populate the structured array
    for i, (timestamp, row) in enumerate(hist.iterrows()):
        stock_data[i] = (timestamp.strftime("%Y-%m-%d %H:%M:%S"), row['Open'], row['High'], 
                         row['Low'], row['Close'], row['Volume'])
    
    return stock_data

def save_to_hdf5(data, timestamp, filename='data.h5'):
    # Save the parsed stock data to an HDF5 file.
    # data (numpy.ndarray): Parsed stock data.
    # timestamp (str): Timestamp for creating a unique group in the HDF5 file.
    # filename (str): Name of the HDF5 file.
    
    hdf5_file_path = os.path.join(data_dir, filename)  # Define the file path for the HDF5 file
    group_name = f"finance/{timestamp}"  # Create a group name with timestamp for unique identification

    with h5py.File(hdf5_file_path, 'a') as h5f:
        # Create a new group for the current timestamp
        group = h5f.create_group(group_name)
        # Create a dataset within the group to store the stock data
        group.create_dataset('stock_info', data=data)

def main(): 
    # Main function to fetch, parse, and save real-time stock data for Ubisoft.
    
    ticker = 'UBI.PA'  # Define the stock ticker symbol for Ubisoft
    try:
        # Fetch the historical stock data
        hist = fetch_stock_info(ticker)

        # Parse the stock data
        stock_data = parse_stock_info(hist)

        if stock_data.size > 0:
            # Generate a timestamp for the current run
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # Save the stock data to an HDF5 file
            save_to_hdf5(stock_data, timestamp)
            logging.info(f"Stock data saved to {os.path.join(data_dir, 'data.h5')} in group finance/{timestamp}")
        else:
            logging.warning("No stock data found")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
