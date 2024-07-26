import os
import logging
from datetime import datetime
import h5py
import pandas as pd

# Configure logging
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=os.path.join(log_dir, 'preprocess_web_search.log'), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Data directory paths
data_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data.h5'))
preprocessed_hdf5_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5'))

# Group name for web search
web_search_group = 'web_search'

def load_web_search_data(hdf5_path, group):
    """Load the latest web search data from the HDF5 file."""
    with h5py.File(hdf5_path, 'r') as hdf:
        subgroups = list(hdf[group].keys())
        if subgroups:
            last_subgroup = subgroups[-1]
            data = hdf[f"{group}/{last_subgroup}/Category: All categories"]
            df = pd.DataFrame(data[:], columns=['count'])
            df = df.iloc[1:].reset_index(drop=True)  # Remove the first entry
            return df, last_subgroup
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
        # Load the web search data
        web_search_data, last_subgroup = load_web_search_data(data_hdf5_path, web_search_group)
        logging.info(f"Loaded web search data with columns: {web_search_data.columns.tolist()}")

        # Count the total daily searches
        total_searches = web_search_data['count'].astype(int).sum()
        preprocessed_web_search_data = pd.DataFrame({
            'total_searches': [total_searches]
        })

        # Save the preprocessed data
        save_preprocessed_data(preprocessed_hdf5_path, web_search_group, preprocessed_web_search_data)
        logging.info(f"Saved preprocessed web search data with columns: {preprocessed_web_search_data.columns.tolist()}")

    except Exception as e:
        logging.error(f"Failed to preprocess web search data: {e}")

if __name__ == "__main__":
    main()
