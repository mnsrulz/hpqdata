import json
import os
import re
import pandas as pd
from datetime import datetime
from pathlib import Path

file_path = './data/lca-downloads-history.json'

release_name = os.getenv("RELEASE_NAME", datetime.now().strftime("%Y-%m-%d %H:%M"))

with open(file_path, 'r') as file:
    data = json.load(file)

# Extract all optionsAssetUrl and name values
quarterly_asset_url_data = [item['assetUrl'] for item in data if 'assetUrl' in item]

print(quarterly_asset_url_data)

dfs = [pd.read_parquet(file, engine="pyarrow") for file in quarterly_asset_url_data]
df = pd.concat(dfs, ignore_index=True)

columns_to_keep = ['RECEIVED_DATE', 'CASE_STATUS', 'EMPLOYER_NAME', 'JOB_TITLE', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY', 'WORKSITE_STATE', 'WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO', 'WAGE_UNIT_OF_PAY']
df = df[columns_to_keep]

df['RECEIVED_DATE_YEAR'] = df['RECEIVED_DATE'].dt.year
df = df.sort_values(by=['RECEIVED_DATE', 'RECEIVED_DATE_YEAR', 'CASE_STATUS', 'EMPLOYER_NAME', 'WORKSITE_STATE', 'JOB_TITLE'])

os.makedirs("temp", exist_ok=True)  # Ensure the 'data' folder exists
output_file = "temp/db.parquet"

df.to_parquet(output_file, compression='zstd', index=False)
# Get the file size in bytes
file_size_bytes = os.path.getsize(output_file)
file_size_mb = file_size_bytes / (1024 * 1024)
print(f"File size after compression: {file_size_mb:.2f} MB")


summary_file = "data/lca-consolidated.json"
# Write updated summary back to the JSON file
with open(summary_file, "w") as file:
    json.dump({"name": release_name, "assetUrl":f"https://github.com/mnsrulz/hpqdata/releases/download/{release_name}/db.parquet"}, file, indent=4)

print(f"Updated summary file: {summary_file}")