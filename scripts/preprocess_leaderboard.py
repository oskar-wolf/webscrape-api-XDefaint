import os
import logging
from datetime import datetime
import h5py
import pandas as pd
import pycountry

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'preprocess_leaderboards.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_leaderboards_data(hdf5_path, group):
    """Load the leaderboards data from the HDF5 file."""
    with h5py.File(hdf5_path, 'r') as hdf:
        subgroups = list(hdf[group].keys())
        latest_subgroup = subgroups[-1]
        data = hdf[f"{group}/{latest_subgroup}/leaderboard"]
        columns = ['rank', 'player', 'country', 'kills', 'matches_played']
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

def country_code_to_name(code):
    """Convert a two-character country code to the full country name."""
    try:
        return pycountry.countries.get(alpha_2=code).name
    except:
        return 'Unknown'

def main():
    data_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data.h5'))
    preprocessed_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5'))
    leaderboards_group = 'leaderboards'
    
    leaderboards_data = load_leaderboards_data(data_hdf5_path, leaderboards_group)
    logging.info(f"Loaded leaderboards data with columns: {leaderboards_data.columns.tolist()}")
    
    # Process the data
    leaderboards_data['rank'] = leaderboards_data['rank'].astype(int)
    leaderboards_data['player'] = leaderboards_data['player'].apply(lambda x: x.decode('utf-8'))
    leaderboards_data['country'] = leaderboards_data['country'].apply(lambda x: x.decode('utf-8'))
    leaderboards_data['country'] = leaderboards_data['country'].apply(lambda x: x.split('/')[-1].split('.')[0] if 'flags' in x else 'N/A')
    leaderboards_data['country'] = leaderboards_data['country'].apply(lambda x: country_code_to_name(x.upper()))
    leaderboards_data['kills'] = leaderboards_data['kills'].astype(int)
    leaderboards_data['matches_played'] = leaderboards_data['matches_played'].astype(int)
    
    save_preprocessed_data(preprocessed_hdf5_path, leaderboards_group, leaderboards_data)
    logging.info("Preprocessing of leaderboards data completed successfully.")

if __name__ == "__main__":
    main()
