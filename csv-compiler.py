import os
import pandas as pd
from tqdm import tqdm  # <-- progress bar

# Set your root directory where the CSV files (and subfolders) are located
root_dir    = '/Users/haquen/Desktop/Raw'
output_base = os.path.join(root_dir, 'processed_batches')

# Create output directory if it doesn't exist
os.makedirs(output_base, exist_ok=True)

# 1) Discover all CSV files
csv_files = []
for dirpath, dirnames, filenames in os.walk(root_dir):
    for fname in filenames:
        if fname.lower().endswith('.csv'):
            csv_files.append(os.path.join(dirpath, fname))
print(f"Found {len(csv_files)} CSV files.")

# 2) Identify common columns (show progress)
column_sets = []
for file in tqdm(csv_files, desc="Reading CSV headers"):
    try:
        df = pd.read_csv(file, nrows=1)
        column_sets.append(set(df.columns))
    except Exception as e:
        tqdm.write(f"Skipping file {file}: {e}")

if column_sets:
    common_columns = set.intersection(*column_sets)
    print(f"Common columns found across all CSVs: {common_columns}")
else:
    print("No valid CSV files found.")
    common_columns = set()

# 3) Process in batches of 500
batch_size = 500
num_batches = (len(csv_files) + batch_size - 1) // batch_size

for batch_idx in tqdm(range(num_batches), desc="Processing batches"):
    start = batch_idx * batch_size
    end   = start + batch_size
    batch_files = csv_files[start:end]
    batch_dfs   = []

    for file in tqdm(batch_files,
                     desc=f" Batch {batch_idx+1}/{num_batches}",
                     leave=False):
        try:
            df = pd.read_csv(file, usecols=common_columns)
            if 'state' in df:
                df = df[df['state'].isin(['successful', 'failed'])]
            else:
                tqdm.write(f" Warning: no 'state' in {file}")
            batch_dfs.append(df)
        except Exception as e:
            tqdm.write(f" Skipping {file}: {e}")

    if batch_dfs:
        batch_df = pd.concat(batch_dfs, ignore_index=True)
        before = batch_df.shape[0]
        batch_df.drop_duplicates(inplace=True)
        after  = batch_df.shape[0]

        print(f"Batch {batch_idx+1}: dropped {before-after} duplicates")

        batch_folder = os.path.join(output_base, f'batch_{batch_idx+1:03}')
        os.makedirs(batch_folder, exist_ok=True)
        out_path = os.path.join(batch_folder, 'combined.csv')
        batch_df.to_csv(out_path, index=False)

        print(f" Saved batch {batch_idx+1} ({batch_df.shape}) to {out_path}")
    else:
        print(f"No data in batch {batch_idx+1}")
