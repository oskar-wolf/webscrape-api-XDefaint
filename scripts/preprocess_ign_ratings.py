import os
import logging
from datetime import datetime
import h5py
import pandas as pd

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'preprocess_ign_ratings.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_ign_ratings_data(hdf5_path, group):
    """Load the IGN ratings data from the HDF5 file."""
    with h5py.File(hdf5_path, 'r') as hdf:
        subgroups = list(hdf[group].keys())
        latest_subgroup = subgroups[-1]
        data = hdf[f"{group}/{latest_subgroup}/ratings"]
        columns = ['timestamp', 'ign_rating', 'user_rating']
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
    ign_ratings_group = 'ign_ratings'
    
    ign_ratings_data = load_ign_ratings_data(data_hdf5_path, ign_ratings_group)
    logging.info(f"Loaded IGN ratings data with columns: {ign_ratings_data.columns.tolist()}")
    
    # Process the data
    ign_ratings_data['timestamp'] = pd.to_datetime(ign_ratings_data['timestamp'].astype(str))
    ign_ratings_data['ign_rating'] = ign_ratings_data['ign_rating'].astype(float)
    ign_ratings_data['user_rating'] = ign_ratings_data['user_rating'].astype(float)
    
    save_preprocessed_data(preprocessed_hdf5_path, ign_ratings_group, ign_ratings_data)
    logging.info("Preprocessing of IGN ratings data completed successfully.")

if __name__ == "__main__":
    main()
