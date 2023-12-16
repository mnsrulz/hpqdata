import pandas

data = ['LCA_Disclosure_Data_FY2021_Q1','LCA_Disclosure_Data_FY2021_Q2', 'LCA_Disclosure_Data_FY2021_Q3','LCA_Disclosure_Data_FY2021_Q4',
        'LCA_Disclosure_Data_FY2022_Q1','LCA_Disclosure_Data_FY2022_Q2','LCA_Disclosure_Data_FY2022_Q3','LCA_Disclosure_Data_FY2022_Q4',
        'LCA_Disclosure_Data_FY2023_Q1', 'LCA_Disclosure_Data_FY2023_Q2', 'LCA_Disclosure_Data_FY2023_Q3', 'LCA_Disclosure_Data_FY2023_Q4']
all_df = []
for x in data:
    print(f'reading excel into dataframe: {x}')
    df = pandas.read_excel(f'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/{x}.xlsx', 
        nrows= 99,converters={
            'EMPLOYER_POSTAL_CODE':str,
            'WORKSITE_POSTAL_CODE':str,
                })
    df = df.loc[:, ['CASE_NUMBER', 
    'CASE_STATUS', 'JOB_TITLE',
    'BEGIN_DATE', 'END_DATE', 'TOTAL_WORKER_POSITIONS', 'NEW_EMPLOYMENT', 'CONTINUED_EMPLOYMENT', 
    'EMPLOYER_NAME',
    #, 'WORKSITE_CITY', 
    'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE',
    'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
    'WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY']]
    
    all_df.append(df)

print('concatenating all data frames')
all_df = pandas.concat(all_df)

print('writing out the parquet file!')
all_df.to_parquet('data/data.parquet')

print('successfully generated the parquet file "data/data.parquet"!')