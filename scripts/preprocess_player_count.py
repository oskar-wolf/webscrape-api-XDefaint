import os
import logging
from datetime import datetime
import h5py
import pandas as pd

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'preprocess_player_counts.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_player_counts_data(hdf5_path, group):
    """Load the player counts data from the HDF5 file."""
    with h5py.File(hdf5_path, 'r') as hdf:
        subgroups = list(hdf[group].keys())
        latest_subgroup = subgroups[-1]
        data = hdf[f"{group}/{latest_subgroup}/data"]
        columns = ['timestamp', 'player_count', 'percentage_change']
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
    player_counts_group = 'player_counts'
    
    player_counts_data = load_player_counts_data(data_hdf5_path, player_counts_group)
    logging.info(f"Loaded player counts data with columns: {player_counts_data.columns.tolist()}")
    
    # Process the data
    player_counts_data['timestamp'] = pd.to_datetime(player_counts_data['timestamp'].apply(lambda x: x.decode('utf-8')))
    player_counts_data['player_count'] = player_counts_data['player_count'].apply(lambda x: float(x.decode('utf-8').replace(',', '')))
    player_counts_data['percentage_change'] = player_counts_data['percentage_change'].apply(lambda x: float(x.decode('utf-8').strip('%')))
    
    save_preprocessed_data(preprocessed_hdf5_path, player_counts_group, player_counts_data)
    logging.info("Preprocessing of player counts data completed successfully.")

if __name__ == "__main__":
    main()
