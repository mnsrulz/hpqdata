import requests
import json
import os
import pandas
from datetime import datetime

import time

summary_file = './data/lca-downloads-history.json'

with open(summary_file, 'r') as file:
    summary_data = json.load(file)
# [{'year': 2021, 'quarter': 1, 'name': 'LCA_Disclosure_Data_FY2021_Q1'}, {'year': 2021, 'quarter': 2, 'name': 'LCA_Disclosure_Data_FY2021_Q2'}]
# print(data)
# print(data)

# sort the data by year and quarter and find the latest
summary_data.sort(key=lambda x: (x['year'], x['quarter']))
latest_quarter = summary_data[-1]
print(latest_quarter)

#assign year and quarter in variables
year = latest_quarter['year']
quarter = latest_quarter['quarter']

if(quarter == 4):
    year = year + 1
    quarter = 1
else:
    quarter = quarter + 1

print(year, quarter)

# release_name = f'RAW_DATA_FY{year}_Q{quarter}'
release_name = os.getenv("RELEASE_NAME", datetime.now().strftime("%Y-%m-%d %H:%M"))

next_quarter_name = f'LCA_Disclosure_Data_FY{year}_Q{quarter}'
next_quarter_file_url = f'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{next_quarter_name}.xlsx'

print(next_quarter_file_url)

# Download the file to temp directory
response = requests.get(next_quarter_file_url)
print(response.status_code)
print(f'Response status code: {response.status_code}')

if response.status_code != 200:
    print('Non 200 OK response code recieved indicating no new files to download. Exiting here!')
    exit()

os.makedirs("temp", exist_ok=True)  # Ensure the 'data' folder exists

temp_excel_summary_file = f'temp/{next_quarter_name}.xlsx'

with open(temp_excel_summary_file, 'wb') as file:
    file.write(response.content)

print(f'File "{ next_quarter_file_url }" downloaded successfully!')

df = pandas.read_excel(temp_excel_summary_file, 
# nrows= 99, 
converters={
    'VISA_CLASS':str,
    'SOC_CODE':str,
    'SOC_TITLE':str,
    'EMPLOYER_NAME':str,
    'EMPLOYER_CITY':str,
    'EMPLOYER_STATE':str,
    'EMPLOYER_POSTAL_CODE':str,
    'WORKSITE_CITY':str,
    'WORKSITE_STATE':str,
    'WORKSITE_POSTAL_CODE':str,
    'JOB_TITLE':str,
    'CASE_STATUS':str,
    'EMPLOYER_ADDRESS1': str,
    'EMPLOYER_ADDRESS2': str
})

print("File loaded successfully into dataframe!")
df = df.loc[:, ['CASE_NUMBER', 'CASE_STATUS', 'RECEIVED_DATE', 'DECISION_DATE', 'ORIGINAL_CERT_DATE', 
        'VISA_CLASS', 'JOB_TITLE', 'SOC_CODE', 'SOC_TITLE', 'FULL_TIME_POSITION', 
        'BEGIN_DATE', 'END_DATE', 'TOTAL_WORKER_POSITIONS', 'NEW_EMPLOYMENT', 'CONTINUED_EMPLOYMENT', 
        'CHANGE_PREVIOUS_EMPLOYMENT', 'NEW_CONCURRENT_EMPLOYMENT', 'CHANGE_EMPLOYER', 'AMENDED_PETITION', 
        'EMPLOYER_NAME', 'EMPLOYER_ADDRESS1', 'EMPLOYER_ADDRESS2', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
        'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE',
        'WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY']]

print("Dumping dataframe to parquet file!")
# Save DataFrames to Parquet files
parquet_summary_file = f'temp/{next_quarter_name}.parquet'
df.to_parquet(parquet_summary_file, index=False)


summary_data.append({"name": next_quarter_name, "year": year, "quarter": quarter, "assetUrl":f"https://github.com/mnsrulz/hpqdata/releases/download/{release_name}/{next_quarter_name}.parquet"})

# Write updated summary back to the JSON file
with open(summary_file, "w") as file:
    json.dump(summary_data, file, indent=4)

print(f"Updated summary file: {summary_file}")