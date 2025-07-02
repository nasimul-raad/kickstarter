import os
import pandas as pd

# Try to import tqdm; if not installed, define a no‐op fallback
try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **kwargs: x

# 1. Configuration
root_dir   = '/Users/haquen/Desktop/Raw'
output_dir = os.path.join(root_dir, 'processed_all')
os.makedirs(output_dir, exist_ok=True)

# 2. Discover all CSV files
csv_files = []
for dirpath, _, filenames in os.walk(root_dir):
    for fname in filenames:
        if fname.lower().endswith('.csv'):
            csv_files.append(os.path.join(dirpath, fname))
print(f"Found {len(csv_files)} CSV files.\n")

# 3. Determine common columns across every file (with progress bar)
column_sets = []
for fpath in tqdm(csv_files, desc="Reading headers"):
    try:
        cols = pd.read_csv(fpath, nrows=0).columns
        column_sets.append(set(cols))
    except Exception as e:
        print(f"  → Skipping header read of {fpath}: {e}")

if not column_sets:
    raise RuntimeError("No CSV headers could be read.")
common_columns = set.intersection(*column_sets)
print(f"\nCommon columns: {common_columns}\n")

# 4. Load, filter on `state`, and collect (with progress bar)
frames = []
for fpath in tqdm(csv_files, desc="Loading & filtering"):
    try:
        df = pd.read_csv(fpath, usecols=common_columns)
        if 'state' in df.columns:
            df = df[df['state'].isin(['successful', 'failed'])]
        else:
            print(f"  ⚠️ 'state' not in {os.path.basename(fpath)} – no filter applied")
        frames.append(df)
    except Exception as e:
        print(f"  → Skipping {fpath}: {e}")

# 5. Concatenate and deduplicate
if not frames:
    raise RuntimeError("No dataframes to concatenate.")
combined = pd.concat(frames, ignore_index=True)
before = combined.shape[0]
combined.drop_duplicates(inplace=True)
after  = combined.shape[0]
print(f"\nDeduplicated: {before - after} rows removed; final shape = {combined.shape}\n")

# 6. Write single output
output_path = os.path.join(output_dir, 'combined_all.csv')
combined.to_csv(output_path, index=False)
print(f"All data saved to {output_path}")
