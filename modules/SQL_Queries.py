import polars as pl
from sqlalchemy import create_engine, text

def create_db_connection():
    # Create a connection engine to the SQLite database
    return create_engine('sqlite:///my_database.db')

def fetch_bank_names_for_year(engine, selected_year):
    # Query the id_rssd for the selected year from the Retail_Table
    query = f"SELECT DISTINCT id_rssd FROM Retail_Table WHERE ActivityYear = {selected_year};"
    df = pl.read_database(query=query, connection=engine.connect())
    id_rssd_list = df['id_rssd'].unique().to_list()

    # Query the bank names for the selected id_rssd from the PE_Table
    query = f"SELECT DISTINCT bank_name FROM PE_Table WHERE id_rssd IN ({', '.join(map(str, id_rssd_list))});"
    df = pl.read_database(query=query, connection=engine.connect())
    return df['bank_name'].unique().to_list()

def fetch_assessment_area(engine, selected_bank, selected_year):
    query = f"SELECT MD_Code, MSA_Code, State_Code, County_Code FROM Retail_Table WHERE id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') AND ActivityYear = {selected_year};"
    df = pl.read_database(query, engine)
    #print(f"Initial query result: {df}")

    assessment_areas = {}

    for row in df.iter_rows():
        md_code, msa_code, state_code, county_code = row
        lookup_method = None  # Reset lookup_method at the start of each loop iteration
        #print(f"Lookup method reset to: {lookup_method}")
        skip_row = False  # Reset skip_row at the start of each loop iteration

        # Look up by MD_Code first
        if md_code is not None and md_code != 'NA':
                #print(f"MD_Code: {md_code}")
                for table in ['2024 tracts', '2022-2023 tracts']:
                    query = f"SELECT `MSA/MD name` FROM `{table}` WHERE `MSA/MD Code` = '{md_code}';"
                    df = pl.read_database(query, engine)
                    if df.height != 0:
                        area_name = df['MSA/MD name'][0]
                        lookup_method = 'md'
                        assessment_areas[area_name] = {'codes': (md_code, msa_code, state_code, county_code, 'md')}
                        #print(f"Used MD_Code {md_code} to look up MSA/MD name {area_name}. Lookup method: {lookup_method}")
                        skip_row = True  # Set skip_row to True if MD_Code lookup was successful
                        break

        # If MD_Code lookup was successful, skip the rest of the current row
        if skip_row:
            #print(f"Skipping row due to successful MD_Code lookup. Current lookup method: {lookup_method}")
            continue

        # If MD_Code lookup failed, try MSA_Code
        if msa_code is not None and msa_code != 'NA':
                #print(f"MSA_Code: {msa_code}")
                for table in ['2024 tracts', '2022-2023 tracts']:
                    query = f"SELECT `MSA/MD name` FROM `{table}` WHERE `MSA/MD Code` = '{msa_code}';"
                    df = pl.read_database(query, engine)
                    if df.height != 0:
                        area_name = df['MSA/MD name'][0]
                        lookup_method = 'msa' 
                        assessment_areas[area_name] = {'codes': (md_code, msa_code, state_code, county_code, 'msa')}
                        #print(f"Used MSA_Code {msa_code} to look up MSA/MD name {area_name}. Lookup method: {lookup_method}")
                        break

        #print(f"After MSA_Code lookup, current lookup method: {lookup_method}")

        # If both MD_Code and MSA_Code lookups failed, try State_Code and County_Code
        if lookup_method != 'md' and lookup_method != 'msa' and str(state_code).isdigit() and str(county_code).isdigit():
            state_code, county_code = map(int, (state_code, county_code))
            #print(f"Looking up by State_Code: {state_code} and County_Code: {county_code}")
            for table in ['2024 tracts', '2022-2023 tracts']:
                query = f"SELECT `County name`, `State` FROM `{table}` WHERE `State code` = {state_code} AND `County code` = {county_code};"
                df = pl.read_database(query, engine)
                if df.height != 0:
                    area_name = f"{df['County name'][0]}, {df['State'][0]}"
                    lookup_method = 'state_county' 
                    assessment_areas[area_name] = {'codes': (md_code, msa_code, state_code, county_code, 'state_county')}
                    #print(f"Used State_Code {state_code} and County_Code {county_code} to look up MSA/MD name {area_name}. Lookup method: {lookup_method}")

        #print(f"After State_Code and County_Code lookup, current lookup method: {lookup_method}")

    if not assessment_areas:
        print("No matching records found")
        return None

    #print(f"Assessment areas: {assessment_areas}")
    return assessment_areas

def fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Print the values of the variables
    #print(f"Engine: {engine}")
    #print(f"Selected bank: {selected_bank}")
    #print(f"Selected year: {selected_year}")
    print(f"MD Code: {md_code}")
    print(f"MSA Code: {msa_code}")
    #print(f"Selected area: {selected_area}")
    print(f"State Code: {state_code}")
    print(f"County Code: {county_code}")
    print(f"Lookup method: {lookup_method}")

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Amt_Orig_SFam_Closed, 
            Amt_Orig_SFam_Open, 
            Amt_Orig_MFam, 
            SF_Amt_Orig, 
            SB_Amt_Orig, 
            Amt_Orig,
            Partial_Ind,
            State_Code,
            County_Code 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Amt_Orig_SFam_Closed, 
            Amt_Orig_SFam_Open, 
            Amt_Orig_MFam, 
            SF_Amt_Orig, 
            SB_Amt_Orig, 
            Amt_Orig,
            Partial_Ind,
            State_Code,
            County_Code 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Amt_Orig_SFam_Closed, 
            Amt_Orig_SFam_Open, 
            Amt_Orig_MFam, 
            SF_Amt_Orig, 
            SB_Amt_Orig, 
            Amt_Orig,
            Partial_Ind,
            State_Code,
            County_Code 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    df = pl.read_database(query, engine)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df

def fetch_loan_data_inside_out(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed_Inside, 
            Loan_Orig_SFam_Open_Inside, 
            Loan_Orig_MFam_Inside, 
            SB_Loan_Orig_Inside, 
            SF_Loan_Orig_Inside, 
            Amt_Orig_SFam_Closed_Inside,
            Amt_Orig_SFam_Open_Inside,
            Amt_Orig_MFam_Inside,
            SB_Amt_Orig_Inside,
            SF_Amt_Orig_Inside,
            Loan_Orig_SFam_Closed,
            Loan_Orig_SFam_Open,
            Loan_Orig_MFam,
            SB_Loan_Orig,
            SF_Loan_Orig,
            Amt_Orig_SFam_Closed,
            Amt_Orig_SFam_Open,
            Amt_Orig_MFam,
            Amt_Orig,
            SB_Amt_Orig,
            SF_Amt_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed_Inside, 
            Loan_Orig_SFam_Open_Inside, 
            Loan_Orig_MFam_Inside, 
            SB_Loan_Orig_Inside, 
            SF_Loan_Orig_Inside, 
            Amt_Orig_SFam_Closed_Inside,
            Amt_Orig_SFam_Open_Inside,
            Amt_Orig_MFam_Inside,
            SB_Amt_Orig_Inside,
            SF_Amt_Orig_Inside,
            Loan_Orig_SFam_Closed,
            Loan_Orig_SFam_Open,
            Loan_Orig_MFam,
            SB_Loan_Orig,
            SF_Loan_Orig,
            Amt_Orig_SFam_Closed,
            Amt_Orig_SFam_Open,
            Amt_Orig_MFam,
            Amt_Orig,
            SB_Amt_Orig,
            SF_Amt_Orig,
            State_Code,
            County_Code 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed_Inside, 
            Loan_Orig_SFam_Open_Inside, 
            Loan_Orig_MFam_Inside, 
            SB_Loan_Orig_Inside, 
            SF_Loan_Orig_Inside, 
            Amt_Orig_SFam_Closed_Inside,
            Amt_Orig_SFam_Open_Inside,
            Amt_Orig_MFam_Inside,
            SB_Amt_Orig_Inside,
            SF_Amt_Orig_Inside,
            Loan_Orig_SFam_Closed,
            Loan_Orig_SFam_Open,
            Loan_Orig_MFam,
            SB_Loan_Orig,
            SF_Loan_Orig,
            Amt_Orig_SFam_Closed,
            Amt_Orig_SFam_Open,
            Amt_Orig_MFam,
            Amt_Orig,
            SB_Amt_Orig,
            SF_Amt_Orig,
            State_Code,
            County_Code 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Debug: Print data_dicts
    #print(f"Data Dicts: {data_dicts}")

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    #print(df.head())

    return df

def fetch_loan_data_bor_income(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed_BILow,
            Loan_Orig_SFam_Closed_BIMod,
            Loan_Orig_SFam_Closed,
            Loan_Orig_SFam_Open_BILow,
            Loan_Orig_SFam_Open_BIMod,
            Loan_Orig_SFam_Open,
            Loan_Orig_BILow,
            Loan_Orig_BIMod,
            Loan_Orig,
            Agg_Loan_Orig_SFam_Closed_BILow,
            Agg_Loan_Orig_SFam_Closed_BIMod,
            Agg_Loan_Orig_SFam_Closed,
            Agg_Loan_Orig_SFam_Open_BILow,
            Agg_Loan_Orig_SFam_Open_BIMod,
            Agg_Loan_Orig_SFam_Open,
            Agg_Loan_Orig_BILow,
            Agg_Loan_Orig_BIMod,
            Agg_Loan_Orig,
            Amt_Orig_SFam_Closed_BILow,
            Amt_Orig_SFam_Closed_BIMod,
            Amt_Orig_SFam_Closed,
            Amt_Orig_SFam_Open_BILow,
            Amt_Orig_SFam_Open_BIMod,
            Amt_Orig_SFam_Open,
            Amt_Orig_BILow,
            Amt_Orig_BIMod,
            Amt_Orig,
            Agg_Amt_Orig_SFam_Closed_BILow,
            Agg_Amt_Orig_SFam_Closed_BIMod,
            Agg_Amt_Orig_SFam_Closed,
            Agg_Amt_Orig_SFam_Open_BILow,
            Agg_Amt_Orig_SFam_Open_BIMod,
            Agg_Amt_Orig_SFam_Open,
            Agg_Amt_Orig_BIMod,
            Agg_Amt_Orig_BILow,
            Agg_Amt_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed_BILow,
            Loan_Orig_SFam_Closed_BIMod,
            Loan_Orig_SFam_Closed,
            Loan_Orig_SFam_Open_BILow,
            Loan_Orig_SFam_Open_BIMod,
            Loan_Orig_SFam_Open,
            Loan_Orig_BILow,
            Loan_Orig_BIMod,
            Loan_Orig,
            Agg_Loan_Orig_SFam_Closed_BILow,
            Agg_Loan_Orig_SFam_Closed_BIMod,
            Agg_Loan_Orig_SFam_Closed,
            Agg_Loan_Orig_SFam_Open_BILow,
            Agg_Loan_Orig_SFam_Open_BIMod,
            Agg_Loan_Orig_SFam_Open,
            Agg_Loan_Orig_BILow,
            Agg_Loan_Orig_BIMod,
            Agg_Loan_Orig,
            Amt_Orig_SFam_Closed_BILow,
            Amt_Orig_SFam_Closed_BIMod,
            Amt_Orig_SFam_Closed,
            Amt_Orig_SFam_Open_BILow,
            Amt_Orig_SFam_Open_BIMod,
            Amt_Orig_SFam_Open,
            Amt_Orig_BILow,
            Amt_Orig_BIMod,
            Amt_Orig,
            Agg_Amt_Orig_SFam_Closed_BILow,
            Agg_Amt_Orig_SFam_Closed_BIMod,
            Agg_Amt_Orig_SFam_Closed,
            Agg_Amt_Orig_SFam_Open_BILow,
            Agg_Amt_Orig_SFam_Open_BIMod,
            Agg_Amt_Orig_SFam_Open,
            Agg_Amt_Orig_BIMod,
            Agg_Amt_Orig_BILow,
            Agg_Amt_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed_BILow,
            Loan_Orig_SFam_Closed_BIMod,
            Loan_Orig_SFam_Closed,
            Loan_Orig_SFam_Open_BILow,
            Loan_Orig_SFam_Open_BIMod,
            Loan_Orig_SFam_Open,
            Loan_Orig_BILow,
            Loan_Orig_BIMod,
            Loan_Orig,
            Agg_Loan_Orig_SFam_Closed_BILow,
            Agg_Loan_Orig_SFam_Closed_BIMod,
            Agg_Loan_Orig_SFam_Closed,
            Agg_Loan_Orig_SFam_Open_BILow,
            Agg_Loan_Orig_SFam_Open_BIMod,
            Agg_Loan_Orig_SFam_Open,
            Agg_Loan_Orig_BILow,
            Agg_Loan_Orig_BIMod,
            Agg_Loan_Orig,
            Amt_Orig_SFam_Closed_BILow,
            Amt_Orig_SFam_Closed_BIMod,
            Amt_Orig_SFam_Closed,
            Amt_Orig_SFam_Open_BILow,
            Amt_Orig_SFam_Open_BIMod,
            Amt_Orig_SFam_Open,
            Amt_Orig_BILow,
            Amt_Orig_BIMod,
            Amt_Orig,
            Agg_Amt_Orig_SFam_Closed_BILow,
            Agg_Amt_Orig_SFam_Closed_BIMod,
            Agg_Amt_Orig_SFam_Closed,
            Agg_Amt_Orig_SFam_Open_BILow,
            Agg_Amt_Orig_SFam_Open_BIMod,
            Agg_Amt_Orig_SFam_Open,
            Agg_Amt_Orig_BIMod,
            Agg_Amt_Orig_BILow,
            Agg_Amt_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Debug: Print data_dicts
    #print(f"Data Dicts: {data_dicts}")

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df

def fetch_loan_data_tract_income(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed,
			Loan_Orig_SFam_Open,
			Loan_Orig,
			Agg_Loan_Orig_SFam_Closed,
			Agg_Loan_Orig_SFam_Open,
			Agg_Loan_Orig,
			Amt_Orig_SFam_Closed,
			Amt_Orig_SFam_Open,
			Amt_Orig,
			Agg_Amt_Orig_SFam_Closed,
			Agg_Amt_Orig_SFam_Open,
			Agg_Amt_Orig,
			Loan_Orig_SFam_Closed_TILow,
			Loan_Orig_SFam_Closed_TIMod,
			Loan_Orig_SFam_Open_TILow,
			Loan_Orig_SFam_Open_TIMod,
			Loan_Orig_MFam_TILow,
			Loan_Orig_MFam_TIMod,
			Loan_Orig_MFam,
			Loan_Orig_TILow,
			Loan_Orig_TIMod,
			Agg_Loan_Orig_SFam_Closed_TILow,
			Agg_Loan_Orig_SFam_Closed_TIMod,
			Agg_Loan_Orig_SFam_Open_TILow,
			Agg_Loan_Orig_SFam_Open_TIMod,
			Agg_Loan_Orig_MFam_TILow,
			Agg_Loan_Orig_MFam_TIMod,
			Agg_Loan_Orig_MFam,
			Agg_Loan_Orig_TILow,
			Agg_Loan_Orig_TIMod,
			Amt_Orig_SFam_Closed_TILow,
			Amt_Orig_SFam_Closed_TIMod,
			Amt_Orig_SFam_Open_TILow,
			Amt_Orig_SFam_Open_TIMod,
			Amt_Orig_MFam_TILow,
			Amt_Orig_MFam_TIMod,
			Amt_Orig_MFam,
			Amt_Orig_TILow,
			Amt_Orig_TIMod,
			Agg_Amt_Orig_SFam_Closed_TILow,
			Agg_Amt_Orig_SFam_Closed_TIMod,
			Agg_Amt_Orig_SFam_Open_TILow,
			Agg_Amt_Orig_SFam_Open_TIMod,
			Agg_Amt_Orig_MFam_TILow,
			Agg_Amt_Orig_MFam_TIMod,
			Agg_Amt_Orig_MFam,
			Agg_Amt_Orig_TILow,
			Agg_Amt_Orig_TIMod,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed,
			Loan_Orig_SFam_Open,
			Loan_Orig,
			Agg_Loan_Orig_SFam_Closed,
			Agg_Loan_Orig_SFam_Open,
			Agg_Loan_Orig,
			Amt_Orig_SFam_Closed,
			Amt_Orig_SFam_Open,
			Amt_Orig,
			Agg_Amt_Orig_SFam_Closed,
			Agg_Amt_Orig_SFam_Open,
			Agg_Amt_Orig,
			Loan_Orig_SFam_Closed_TILow,
			Loan_Orig_SFam_Closed_TIMod,
			Loan_Orig_SFam_Open_TILow,
			Loan_Orig_SFam_Open_TIMod,
			Loan_Orig_MFam_TILow,
			Loan_Orig_MFam_TIMod,
			Loan_Orig_MFam,
			Loan_Orig_TILow,
			Loan_Orig_TIMod,
			Agg_Loan_Orig_SFam_Closed_TILow,
			Agg_Loan_Orig_SFam_Closed_TIMod,
			Agg_Loan_Orig_SFam_Open_TILow,
			Agg_Loan_Orig_SFam_Open_TIMod,
			Agg_Loan_Orig_MFam_TILow,
			Agg_Loan_Orig_MFam_TIMod,
			Agg_Loan_Orig_MFam,
			Agg_Loan_Orig_TILow,
			Agg_Loan_Orig_TIMod,
			Amt_Orig_SFam_Closed_TILow,
			Amt_Orig_SFam_Closed_TIMod,
			Amt_Orig_SFam_Open_TILow,
			Amt_Orig_SFam_Open_TIMod,
			Amt_Orig_MFam_TILow,
			Amt_Orig_MFam_TIMod,
			Amt_Orig_MFam,
			Amt_Orig_TILow,
			Amt_Orig_TIMod,
			Agg_Amt_Orig_SFam_Closed_TILow,
			Agg_Amt_Orig_SFam_Closed_TIMod,
			Agg_Amt_Orig_SFam_Open_TILow,
			Agg_Amt_Orig_SFam_Open_TIMod,
			Agg_Amt_Orig_MFam_TILow,
			Agg_Amt_Orig_MFam_TIMod,
			Agg_Amt_Orig_MFam,
			Agg_Amt_Orig_TILow,
			Agg_Amt_Orig_TIMod,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Loan_Orig_SFam_Closed,
			Loan_Orig_SFam_Open,
			Loan_Orig,
			Agg_Loan_Orig_SFam_Closed,
			Agg_Loan_Orig_SFam_Open,
			Agg_Loan_Orig,
			Amt_Orig_SFam_Closed,
			Amt_Orig_SFam_Open,
			Amt_Orig,
			Agg_Amt_Orig_SFam_Closed,
			Agg_Amt_Orig_SFam_Open,
			Agg_Amt_Orig,
			Loan_Orig_SFam_Closed_TILow,
			Loan_Orig_SFam_Closed_TIMod,
			Loan_Orig_SFam_Open_TILow,
			Loan_Orig_SFam_Open_TIMod,
			Loan_Orig_MFam_TILow,
			Loan_Orig_MFam_TIMod,
			Loan_Orig_MFam,
			Loan_Orig_TILow,
			Loan_Orig_TIMod,
			Agg_Loan_Orig_SFam_Closed_TILow,
			Agg_Loan_Orig_SFam_Closed_TIMod,
			Agg_Loan_Orig_SFam_Open_TILow,
			Agg_Loan_Orig_SFam_Open_TIMod,
			Agg_Loan_Orig_MFam_TILow,
			Agg_Loan_Orig_MFam_TIMod,
			Agg_Loan_Orig_MFam,
			Agg_Loan_Orig_TILow,
			Agg_Loan_Orig_TIMod,
			Amt_Orig_SFam_Closed_TILow,
			Amt_Orig_SFam_Closed_TIMod,
			Amt_Orig_SFam_Open_TILow,
			Amt_Orig_SFam_Open_TIMod,
			Amt_Orig_MFam_TILow,
			Amt_Orig_MFam_TIMod,
			Amt_Orig_MFam,
			Amt_Orig_TILow,
			Amt_Orig_TIMod,
			Agg_Amt_Orig_SFam_Closed_TILow,
			Agg_Amt_Orig_SFam_Closed_TIMod,
			Agg_Amt_Orig_SFam_Open_TILow,
			Agg_Amt_Orig_SFam_Open_TIMod,
			Agg_Amt_Orig_MFam_TILow,
			Agg_Amt_Orig_MFam_TIMod,
			Agg_Amt_Orig_MFam,
			Agg_Amt_Orig_TILow,
			Agg_Amt_Orig_TIMod,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Debug: Print data_dicts
    #print(f"Data Dicts: {data_dicts}")

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df

def fetch_loan_data_business(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            SB_Loan_Orig_TILow,
			SB_Loan_Orig_TIMod,
			SB_Loan_Orig,
			Agg_SB_Loan_Purch_TILow,
			Agg_SB_Loan_Orig_TIMod,
			Agg_SB_Loan_Orig,
			SF_Loan_Orig_TILow,
			SF_Loan_Orig_TIMod,
			SF_Loan_Orig,
			Agg_SF_Loan_Orig_TILow,
			Agg_SF_Loan_Orig_TIMod,
			Agg_SF_Loan_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            SB_Loan_Orig_TILow,
			SB_Loan_Orig_TIMod,
			SB_Loan_Orig,
			Agg_SB_Loan_Purch_TILow,
			Agg_SB_Loan_Orig_TIMod,
			Agg_SB_Loan_Orig,
			SF_Loan_Orig_TILow,
			SF_Loan_Orig_TIMod,
			SF_Loan_Orig,
			Agg_SF_Loan_Orig_TILow,
			Agg_SF_Loan_Orig_TIMod,
			Agg_SF_Loan_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            SB_Loan_Orig_TILow,
			SB_Loan_Orig_TIMod,
			SB_Loan_Orig,
			Agg_SB_Loan_Purch_TILow,
			Agg_SB_Loan_Orig_TIMod,
			Agg_SB_Loan_Orig,
			SF_Loan_Orig_TILow,
			SF_Loan_Orig_TIMod,
			SF_Loan_Orig,
			Agg_SF_Loan_Orig_TILow,
			Agg_SF_Loan_Orig_TIMod,
			Agg_SF_Loan_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df

def fetch_loan_business_size(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            SB_Loan_Orig_GAR_less_1m,
			SB_Loan_Orig,
			SF_Loan_Orig_GAR_less_1m,
			SF_Loan_Orig,
			Agg_SB_Loan_Orig_GAR_less_1m,
			Agg_SB_Loan_Orig,
			Agg_SF_Loan_Orig_GAR_less_1m,
			Agg_SF_Loan_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            SB_Loan_Orig_GAR_less_1m,
			SB_Loan_Orig,
			SF_Loan_Orig_GAR_less_1m,
			SF_Loan_Orig,
			Agg_SB_Loan_Orig_GAR_less_1m,
			Agg_SB_Loan_Orig,
			Agg_SF_Loan_Orig_GAR_less_1m,
			Agg_SF_Loan_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            SB_Loan_Orig_GAR_less_1m,
			SB_Loan_Orig,
			SF_Loan_Orig_GAR_less_1m,
			SF_Loan_Orig,
			Agg_SB_Loan_Orig_GAR_less_1m,
			Agg_SB_Loan_Orig,
			Agg_SF_Loan_Orig_GAR_less_1m,
			Agg_SF_Loan_Orig,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df

def fetch_demographics(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Owner_Occupied_Units_TILow_Inside,
			Owner_Occupied_Units_TIMod_Inside,
			Owner_Occupied_Units_Inside,
			Total5orMoreHousingUnitsInStructure_TILow_Inside,
			Total5orMoreHousingUnitsInStructure_TIMod_Inside,
			Total5orMoreHousingUnitsInStructure_Inside,
			Low_Income_Family_Count_Inside,
			Moderate_Income_Family_Count_Inside,
			Family_Count_Inside,
			Owner_Occupied_Units_TILow,
			Owner_Occupied_Units_TIMod,
			Owner_Occupied_Units,
			Total5orMoreHousingUnitsInStructure_TILow,
			Total5orMoreHousingUnitsInStructure_TIMod,
			Total5orMoreHousingUnitsInStructure,
			Low_Income_Family_Count,
			Moderate_Income_Family_Count,
			Family_Count,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Owner_Occupied_Units_TILow_Inside,
			Owner_Occupied_Units_TIMod_Inside,
			Owner_Occupied_Units_Inside,
			Total5orMoreHousingUnitsInStructure_TILow_Inside,
			Total5orMoreHousingUnitsInStructure_TIMod_Inside,
			Total5orMoreHousingUnitsInStructure_Inside,
			Low_Income_Family_Count_Inside,
			Moderate_Income_Family_Count_Inside,
			Family_Count_Inside,
			Owner_Occupied_Units_TILow,
			Owner_Occupied_Units_TIMod,
			Owner_Occupied_Units,
			Total5orMoreHousingUnitsInStructure_TILow,
			Total5orMoreHousingUnitsInStructure_TIMod,
			Total5orMoreHousingUnitsInStructure,
			Low_Income_Family_Count,
			Moderate_Income_Family_Count,
			Family_Count,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Owner_Occupied_Units_TILow_Inside,
			Owner_Occupied_Units_TIMod_Inside,
			Owner_Occupied_Units_Inside,
			Total5orMoreHousingUnitsInStructure_TILow_Inside,
			Total5orMoreHousingUnitsInStructure_TIMod_Inside,
			Total5orMoreHousingUnitsInStructure_Inside,
			Low_Income_Family_Count_Inside,
			Moderate_Income_Family_Count_Inside,
			Family_Count_Inside,
			Owner_Occupied_Units_TILow,
			Owner_Occupied_Units_TIMod,
			Owner_Occupied_Units,
			Total5orMoreHousingUnitsInStructure_TILow,
			Total5orMoreHousingUnitsInStructure_TIMod,
			Total5orMoreHousingUnitsInStructure,
			Low_Income_Family_Count,
			Moderate_Income_Family_Count,
			Family_Count,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Debug: Print data_dicts
    #print(f"Data Dicts: {data_dicts}")

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df


def fetch_bus_demographics(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        #print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Establishments_Small_Business_TILow_Inside,
			Establishments_Small_Business_TIMod_Inside,
			Establishments_Small_Business_Inside,
			Establishments_GAR_Less_1M_Small_Business_Inside,
			Establishments_GAR_1_to_250k_Small_Business_Inside,
			Establishments_GAR_250k_to_1M_Small_Business_Inside,
			Establishments_Small_Farm_TILow_Inside,
			Establishments_Small_Farm_TIMod_Inside,
			Establishments_Small_Farm_Inside,
			Establishments_GAR_Less_1M_Small_Farm_Inside,
			Establishments_GAR_1_to_250k_Small_Farm_Inside,
			Establishments_GAR_250k_to_1M_Small_Farm_Inside,
			Establishments_Small_Business_TILow,
			Establishments_Small_Business_TIMod,
			Establishments_Small_Business,
			Establishments_GAR_Less_1M_Small_Business,
			Establishments_GAR_1_to_250k_Small_Business,
			Establishments_GAR_250k_to_1M_Small_Business,
			Establishments_Small_Farm_TILow,
			Establishments_Small_Farm_TIMod,
			Establishments_Small_Farm,
			Establishments_GAR_Less_1M_Small_Farm,
			Establishments_GAR_1_to_250k_Small_Farm,
			Establishments_GAR_250k_to_1M_Small_Farm,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        #print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Establishments_Small_Business_TILow_Inside,
			Establishments_Small_Business_TIMod_Inside,
			Establishments_Small_Business_Inside,
			Establishments_GAR_Less_1M_Small_Business_Inside,
			Establishments_GAR_1_to_250k_Small_Business_Inside,
			Establishments_GAR_250k_to_1M_Small_Business_Inside,
			Establishments_Small_Farm_TILow_Inside,
			Establishments_Small_Farm_TIMod_Inside,
			Establishments_Small_Farm_Inside,
			Establishments_GAR_Less_1M_Small_Farm_Inside,
			Establishments_GAR_1_to_250k_Small_Farm_Inside,
			Establishments_GAR_250k_to_1M_Small_Farm_Inside,
			Establishments_Small_Business_TILow,
			Establishments_Small_Business_TIMod,
			Establishments_Small_Business,
			Establishments_GAR_Less_1M_Small_Business,
			Establishments_GAR_1_to_250k_Small_Business,
			Establishments_GAR_250k_to_1M_Small_Business,
			Establishments_Small_Farm_TILow,
			Establishments_Small_Farm_TIMod,
			Establishments_Small_Farm,
			Establishments_GAR_Less_1M_Small_Farm,
			Establishments_GAR_1_to_250k_Small_Farm,
			Establishments_GAR_250k_to_1M_Small_Farm,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        #print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Establishments_Small_Business_TILow_Inside,
			Establishments_Small_Business_TIMod_Inside,
			Establishments_Small_Business_Inside,
			Establishments_GAR_Less_1M_Small_Business_Inside,
			Establishments_GAR_1_to_250k_Small_Business_Inside,
			Establishments_GAR_250k_to_1M_Small_Business_Inside,
			Establishments_Small_Farm_TILow_Inside,
			Establishments_Small_Farm_TIMod_Inside,
			Establishments_Small_Farm_Inside,
			Establishments_GAR_Less_1M_Small_Farm_Inside,
			Establishments_GAR_1_to_250k_Small_Farm_Inside,
			Establishments_GAR_250k_to_1M_Small_Farm_Inside,
			Establishments_Small_Business_TILow,
			Establishments_Small_Business_TIMod,
			Establishments_Small_Business,
			Establishments_GAR_Less_1M_Small_Business,
			Establishments_GAR_1_to_250k_Small_Business,
			Establishments_GAR_250k_to_1M_Small_Business,
			Establishments_Small_Farm_TILow,
			Establishments_Small_Farm_TIMod,
			Establishments_Small_Farm,
			Establishments_GAR_Less_1M_Small_Farm,
			Establishments_GAR_1_to_250k_Small_Farm,
			Establishments_GAR_250k_to_1M_Small_Farm,
            State_Code,
            County_Code
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND State_Code = {state_code} AND County_Code = {county_code};
        """
    # Create a Polars DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    # Create a list of dictionaries for constructing the DataFrame
    data_dicts = [dict(zip(columns, row)) for row in rows]

    # Debug: Print data_dicts
    #print(f"Data Dicts: {data_dicts}")

    # Create the Polars DataFrame
    df = pl.DataFrame(data_dicts)

    # Print the first few rows of the DataFrame
    print(df.head())

    return df