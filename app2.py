from pathlib import Path
import pandas as pd

data_dir = Path('d')
full_df = pd.concat(
    pd.read_parquet(parquet_file)
    for parquet_file in data_dir.rglob('*.parquet')
)

full_df['RECEIVED_DATE_YEAR'] = full_df['RECEIVED_DATE'].dt.year
full_df.sort_values(by='RECEIVED_DATE', ascending=False, inplace=True)
print(full_df.RECEIVED_DATE_YEAR.dtype)
print(full_df.head)
#full_df.to_parquet('db_full.parquet')