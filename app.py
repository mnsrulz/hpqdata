import pandas

data = ['LCA_Disclosure_Data_FY2021_Q1','LCA_Disclosure_Data_FY2021_Q2', 'LCA_Disclosure_Data_FY2021_Q3','LCA_Disclosure_Data_FY2021_Q4',
        'LCA_Disclosure_Data_FY2022_Q1','LCA_Disclosure_Data_FY2022_Q2','LCA_Disclosure_Data_FY2022_Q3','LCA_Disclosure_Data_FY2022_Q4',
        'LCA_Disclosure_Data_FY2023_Q1', 'LCA_Disclosure_Data_FY2023_Q2', 'LCA_Disclosure_Data_FY2023_Q3', 'LCA_Disclosure_Data_FY2023_Q4']
all_df = []
for x in data[:1]:
    print(f'reading excel into dataframe: {x}')
    df = pandas.read_excel(f'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{x}.xlsx', 
        nrows= 99,converters={
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
    df = df.loc[:, ['CASE_NUMBER', 'CASE_STATUS', 'RECEIVED_DATE', 'DECISION_DATE', 'ORIGINAL_CERT_DATE', 
                    'VISA_CLASS', 'JOB_TITLE', 'SOC_CODE', 'SOC_TITLE', 'FULL_TIME_POSITION', 
                    'BEGIN_DATE', 'END_DATE', 'TOTAL_WORKER_POSITIONS', 'NEW_EMPLOYMENT', 'CONTINUED_EMPLOYMENT', 
                    'CHANGE_PREVIOUS_EMPLOYMENT', 'NEW_CONCURRENT_EMPLOYMENT', 'CHANGE_EMPLOYER', 'AMENDED_PETITION', 
                    'EMPLOYER_NAME', 'EMPLOYER_ADDRESS1', 'EMPLOYER_ADDRESS2', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
                    'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE',
                    'WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY']]
    print(df.columns.tolist());
    # all_df.append(df)

print('concatenating all data frames')
# all_df = pandas.concat(all_df)

print('writing out the parquet file!')
# all_df.to_parquet('data/data.parquet')

# print('successfully generated the parquet file "data/data.parquet"!')