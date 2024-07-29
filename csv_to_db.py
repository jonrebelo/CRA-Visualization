import pandas as pd
from sqlalchemy import create_engine
import os
from multiprocessing import Pool

# Load the CSV file into a DataFrame
df = pd.read_csv('data/performance_evaluation_table.csv')

# Create a copy of the DataFrame before making changes
df_before = df.copy()

# Convert all strings in column 1 to uppercase
df.iloc[:, 1] = df.iloc[:, 1].astype(str).str.upper()

# Replace standalone "AND" with "&"
df.iloc[:, 1] = df.iloc[:, 1].str.replace(r"\bAND\b", "&")

# Sort by 'id_rssd' and 'exam_year' in descending order
df.sort_values(by=['id_rssd', 'exam_year'], ascending=[True, False], inplace=True)

# Replace 'bank_name' with the 'bank_name' of the latest 'exam_year' in each 'id_rssd' group
df['bank_name'] = df.groupby('id_rssd')['bank_name'].transform('first')

# Reset index for both DataFrames
df.reset_index(drop=True, inplace=True)
df_before.reset_index(drop=True, inplace=True)

# Compare the two DataFrames and print the changes
changes = df_before[df_before['bank_name'] != df['bank_name']]
for index, row in changes.iterrows():
    print(f"Changed name from {df_before.loc[index, 'bank_name']} to {row['bank_name']} for id_rssd {row['id_rssd']}")

# Save the DataFrame back to the CSV file
df.to_csv('data/performance_evaluation_table.csv', index=False)

# Define the column types for each CSV file
column_types = {
    'Retail_Table': {
        'string': ['id_rssd', 'cra_respondent_id', 'cra_agency_code', 'hmda_lender_id', 'hmda_agency_code', 'Lender_in_CRA', 'Lender_in_HMDA', 'MSA_Code', 'MD_Code', 'State_Code', 'County_Code', 'Partial_Ind'],
        'int': [],
    },
    'PE_Table': {
        'string': ['agency', 'bank_name', 'bank_identifier', 'id_rssd', 'exam_proc', 'exam_scope', 'Lender_in_HMDA', 'Lender_in_CRA', 'assessment_area', 'State_Code', 'County_Code', 'MSA_Code', 'assessment_area_type', 'assessment_area_subtype', 'cd_only', 'overall_rating', 'lending_test_rating', 'cd_test_rating', 'investment_test_rating', 'statistical_sample'],
        'int': [],
        'date': ['mort_eval_period_start', 'mort_eval_period_end', 'cra_eval_period_start', 'cra_eval_period_end', 'cd_eval_period_start', 'cd_eval_period_end', 'consumer_eval_period_start', 'consumer_eval_period_end', 'exam_start_date']
    },
    '2024_tracts': {
        'string': ['Year', 'MSA/MD code type', 'MSA/MD code', 'State code', 'County code', 'Tract', 'MSA/MD name', 'State', 'County name', 'FIPS code', 'MSA/MD MFI', 'Tract MFI', 'Tract income percentage', 'Tract income level'],
        'int': [],
        'date': []
    },
    '2022_2023_tracts': {
        'string': ['Year', 'MSA/MD code type', 'MSA/MD code', 'State code', 'County code', 'Tract', 'MSA/MD name', 'State', 'County name', 'FIPS code', 'MSA/MD MFI', 'Tract MFI', 'Tract income percentage', 'Tract income level'],
        'int': [],
        'date': []
    }
}


def process_csv(csv_file):
    # Create a connection engine to the SQLite database
    engine = create_engine('sqlite:///my_database.db')

    # Generate a table name from the first word of the CSV file name
    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    # Get the column types for this CSV file
    types = column_types[table_name]

    # Define the date format (input format in the CSV)
    date_format = '%Y-%m-%d'

    # Read and write the CSV file in chunks
    chunksize = 150000  # adjust this value depending on your system's memory
    for chunk in pd.read_csv(csv_file, chunksize=chunksize, keep_default_na=False, na_values=['NA'], low_memory=False):
        # Convert the columns to the correct types
        for col in types['string']:
            chunk[col] = chunk[col].astype(str).fillna('NA')  # Ensure 'NA' is treated as string
        for col in types['int']:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0).astype(int)  # 'NA' becomes 0
        for col in types.get('date', []):
            chunk[col] = pd.to_datetime(chunk[col], format=date_format, errors='coerce')  # Convert to datetime
            chunk[col] = chunk[col].fillna(pd.Timestamp.min)  # Handle 'NA' in date columns, setting to a minimum date
            chunk[col] = chunk[col].dt.strftime('%m/%d/%Y')  # Format dates as MM/DD/YYYY

        chunk.to_sql(table_name, engine, if_exists='append', index=False)

def process_excel(sheet_name, table_name):
    # Create a connection engine to the SQLite database
    engine = create_engine('sqlite:///my_database.db')

    # Get the column types for this sheet
    types = column_types[table_name]

    # Read the Excel sheet into a DataFrame
    df = pd.read_excel('data/MSA_state_county_tract.xlsx', sheet_name=sheet_name)

    # Convert the columns to the correct types
    for col in types['string']:
        df[col] = df[col].astype(str).fillna('NA')  # Ensure 'NA' is treated as string
    for col in types['int']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)  # 'NA' becomes 0
    for col in types.get('date', []):
        df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert to datetime
        df[col] = df[col].fillna(pd.Timestamp.min)  # Handle 'NA' in date columns, setting to a minimum date
        df[col] = df[col].dt.strftime('%m/%d/%Y')  # Format dates as MM/DD/YYYY

    df.to_sql(table_name, engine, if_exists='append', index=False)

def generate_excel_output(engine, table_names):
    for table_name in table_names:
        df = pd.read_sql_table(table_name, engine)

        # Check if all cells in a column are of the same data type
        mixed_type_cols = df.applymap(type).nunique()
        mixed_type_cols = mixed_type_cols[mixed_type_cols > 1]

        # Create a DataFrame from df.dtypes
        dtypes_df = df.dtypes.reset_index()
        dtypes_df.columns = ['Column Name', 'Data Type']

        # Add a new column to dtypes_df for mixed data types
        dtypes_df['Mixed Types'] = dtypes_df['Column Name'].apply(lambda x: ', '.join(set(df[x].map(type).astype(str))) if x in mixed_type_cols else 'No')

        # Output to Excel
        dtypes_df.to_excel(f"{table_name}_dtypes.xlsx", index=False)

if __name__ == '__main__':
    # List of CSV files
    csv_files = [
        'data/retail_loan_lending_test_table.csv',
        'data/performance_evaluation_table.csv'
    ]

    excel_sheets = {
        '2024 tracts': '2024_tracts',
        '2022-2023 tracts': '2022_2023_tracts'
    }

    # Create a pool of worker processes
    with Pool() as p:
        p.map(process_csv, csv_files)

    # Generate the Excel output after all CSV files have been processed
    engine = create_engine('sqlite:///my_database.db')
    table_names = [os.path.splitext(os.path.basename(csv_file))[0] for csv_file in csv_files]
    generate_excel_output(engine, table_names)
