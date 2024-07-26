import os
import logging
from datetime import datetime
import h5py
import pandas as pd

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'preprocess_reddit_posts.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Data directory paths
data_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data.h5'))
preprocessed_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5'))

# Group name for reddit posts
reddit_posts_group = 'reddit_XDefiant_posts'

def load_reddit_posts_data(hdf5_path, group):
    """Load the latest reddit posts data from the HDF5 file."""
    with h5py.File(hdf5_path, 'r') as hdf:
        subgroups = list(hdf[group].keys())
        if subgroups:
            last_subgroup = subgroups[-1]
            data = hdf[f"{group}/{last_subgroup}/data"]
            columns = data.attrs['columns']
            df = pd.DataFrame(data[:], columns=columns)
            return df
        else:
            raise ValueError(f"No subgroups found in {group}")

def save_preprocessed_data(hdf5_path, group, df):
    """Save the preprocessed data to a new HDF5 file."""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    with h5py.File(hdf5_path, 'a') as hdf:
        grp = hdf.require_group(f"{group}/{timestamp}")
        ds = grp.create_dataset('preprocessed_data', data=df.astype(str).to_numpy(), dtype=h5py.string_dtype(encoding='utf-8'))
        ds.attrs['columns'] = list(df.columns)

def main():
    try:
        # Load the reddit posts data
        reddit_posts_data = load_reddit_posts_data(data_hdf5_path, reddit_posts_group)
        logging.info(f"Loaded reddit posts data with columns: {reddit_posts_data.columns.tolist()}")

        # Select the required columns
        sentiment_columns = ['title', 'textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score']
        preprocessed_reddit_posts_data = reddit_posts_data[sentiment_columns]

        # Save the preprocessed data
        save_preprocessed_data(preprocessed_hdf5_path, reddit_posts_group, preprocessed_reddit_posts_data)
        logging.info(f"Saved preprocessed reddit posts data with columns: {preprocessed_reddit_posts_data.columns.tolist()}")

    except Exception as e:
        logging.error(f"Failed to preprocess reddit posts data: {e}")

if __name__ == "__main__":
    main()
