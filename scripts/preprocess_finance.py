import os
import logging
from datetime import datetime
import h5py
import pandas as pd

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'preprocess_finance.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_finance_data(hdf5_path, group):
    """Load the finance data from the HDF5 file."""
    with h5py.File(hdf5_path, 'r') as hdf:
        subgroups = list(hdf[group].keys())
        latest_subgroup = subgroups[-1]
        data = hdf[f"{group}/{latest_subgroup}/stock_info"]
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(data[:], columns=columns)
    return df

def save_preprocessed_data(hdf5_path, group, df):
    """Save the preprocessed data to the HDF5 file."""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    with h5py.File(hdf5_path, 'a') as hdf:
        grp = hdf.create_group(f"{group}/{timestamp}")
        ds = grp.create_dataset('preprocessed_data', data=df.astype(str).to_numpy(), dtype=h5py.string_dtype(encoding='utf-8'))
        ds.attrs['columns'] = list(df.columns)
    logging.info(f"Saved preprocessed data to HDF5 group: {group}/{timestamp}")

def main():
    data_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data.h5'))
    preprocessed_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5'))
    finance_group = 'finance'
    
    finance_data = load_finance_data(data_hdf5_path, finance_group)
    logging.info(f"Loaded finance data with columns: {finance_data.columns.tolist()}")
    
    # Process the data
    finance_data['timestamp'] = pd.to_datetime(finance_data['timestamp'].astype(str))
    finance_data['open'] = finance_data['open'].astype(float)
    finance_data['high'] = finance_data['high'].astype(float)
    finance_data['low'] = finance_data['low'].astype(float)
    finance_data['close'] = finance_data['close'].astype(float)
    finance_data['volume'] = finance_data['volume'].astype(float)
    
    save_preprocessed_data(preprocessed_hdf5_path, finance_group, finance_data)
    logging.info("Preprocessing of finance data completed successfully.")

if __name__ == "__main__":
    main()
