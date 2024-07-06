import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import polars as pl
from sqlalchemy import create_engine

def create_db_connection():
    # Create a connection engine to the SQLite database
    return create_engine('sqlite:///my_database.db')

def fetch_bank_names(engine):
    # Query all bank names from the PE_Table
    query = "SELECT DISTINCT bank_name FROM PE_Table;"
    # Use polars to read the SQL query
    df = pl.read_database(query=query, connection=engine.connect())
    return df['bank_name'].unique().to_list()

def fetch_years_for_bank(engine, selected_bank):
    # Query the id_rssd for the selected bank from the PE_Table
    query = f"SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}';"
    df = pl.read_database(query=query, connection=engine.connect())
    id_rssd = df['id_rssd'][0]
    # Query the available years for the selected bank from the Retail_Table
    query = f"SELECT DISTINCT ActivityYear FROM Retail_Table WHERE id_rssd = {id_rssd};"
    df = pl.read_database(query=query, connection=engine.connect())
    return df['ActivityYear'].unique().to_list()

def fetch_assessment_area(engine, selected_bank, selected_year):
    query = f"SELECT MD_Code, MSA_Code, State_Code, County_Code FROM Retail_Table WHERE id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') AND ActivityYear = {selected_year};"
    df = pl.read_database(query, engine)
    print(f"Initial query result: {df}")

    assessment_areas = {}

    for row in df.iter_rows():
        md_code, msa_code, state_code, county_code = row
        lookup_method = None  # Reset lookup_method at the start of each loop iteration
        print(f"Lookup method reset to: {lookup_method}")
        skip_row = False  # Reset skip_row at the start of each loop iteration

        # Look up by MD_Code first
        if md_code is not None and md_code != 'NA':
                print(f"MD_Code: {md_code}")
                for table in ['2024 tracts', '2022-2023 tracts']:
                    query = f"SELECT `MSA/MD name` FROM `{table}` WHERE `MSA/MD Code` = '{md_code}';"
                    df = pl.read_database(query, engine)
                    if df.height != 0:
                        area_name = df['MSA/MD name'][0]
                        lookup_method = 'md'
                        assessment_areas[area_name] = {'codes': (md_code, msa_code, state_code, county_code, 'md')}
                        print(f"Used MD_Code {md_code} to look up MSA/MD name {area_name}. Lookup method: {lookup_method}")
                        skip_row = True  # Set skip_row to True if MD_Code lookup was successful
                        break

        # If MD_Code lookup was successful, skip the rest of the current row
        if skip_row:
            print(f"Skipping row due to successful MD_Code lookup. Current lookup method: {lookup_method}")
            continue

        # If MD_Code lookup failed, try MSA_Code
        if msa_code is not None and msa_code != 'NA':
                print(f"MSA_Code: {msa_code}")
                for table in ['2024 tracts', '2022-2023 tracts']:
                    query = f"SELECT `MSA/MD name` FROM `{table}` WHERE `MSA/MD Code` = '{msa_code}';"
                    df = pl.read_database(query, engine)
                    if df.height != 0:
                        area_name = df['MSA/MD name'][0]
                        lookup_method = 'msa' 
                        assessment_areas[area_name] = {'codes': (md_code, msa_code, state_code, county_code, 'msa')}
                        print(f"Used MSA_Code {msa_code} to look up MSA/MD name {area_name}. Lookup method: {lookup_method}")
                        break

        print(f"After MSA_Code lookup, current lookup method: {lookup_method}")

        # If both MD_Code and MSA_Code lookups failed, try State_Code and County_Code
        if lookup_method != 'md' and lookup_method != 'msa' and str(state_code).isdigit() and str(county_code).isdigit():
            state_code, county_code = map(int, (state_code, county_code))
            print(f"Looking up by State_Code: {state_code} and County_Code: {county_code}")
            for table in ['2024 tracts', '2022-2023 tracts']:
                query = f"SELECT `County name`, `State` FROM `{table}` WHERE `State code` = {state_code} AND `County code` = {county_code};"
                df = pl.read_database(query, engine)
                if df.height != 0:
                    area_name = f"{df['County name'][0]}, {df['State'][0]}"
                    lookup_method = 'state_county' 
                    assessment_areas[area_name] = {'codes': (md_code, msa_code, state_code, county_code, 'state_county')}
                    print(f"Used State_Code {state_code} and County_Code {county_code} to look up MSA/MD name {area_name}. Lookup method: {lookup_method}")

        print(f"After State_Code and County_Code lookup, current lookup method: {lookup_method}")

    if not assessment_areas:
        print("No matching records found")
        return None

    print(f"Assessment areas: {assessment_areas}")
    return assessment_areas

def fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

    # Print the values of the variables
    print(f"Engine: {engine}")
    print(f"Selected bank: {selected_bank}")
    print(f"Selected year: {selected_year}")
    print(f"MD Code: {md_code}")
    print(f"MSA Code: {msa_code}")
    print(f"Selected area: {selected_area}")
    print(f"State Code: {state_code}")
    print(f"County Code: {county_code}")
    print(f"Lookup method: {lookup_method}")

    # Query the loan data for the selected bank, year, and assessment area from the Retail_Table
    if lookup_method == 'md':
        print("Using MD Code for lookup")
        query = f"""
        SELECT 
            Amt_Orig_SFam_Closed, 
            Amt_Orig_SFam_Open, 
            Amt_Orig_Mfam, 
            SF_Amt_Orig, 
            SB_Amt_Orig, 
            Amt_Orig 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MD_Code = '{md_code}';
        """
    elif lookup_method == 'msa':
        print("Using MSA Code for lookup")
        query = f"""
        SELECT 
            Amt_Orig_SFam_Closed, 
            Amt_Orig_SFam_Open, 
            Amt_Orig_Mfam, 
            SF_Amt_Orig, 
            SB_Amt_Orig, 
            Amt_Orig 
        FROM Retail_Table 
        WHERE 
            id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}') 
            AND ActivityYear = {selected_year} 
            AND MSA_Code = '{msa_code}';
        """
    else:  # lookup_method == 'state_county'
        print("Using State and County Code for lookup")
        query = f"""
        SELECT 
            Amt_Orig_SFam_Closed, 
            Amt_Orig_SFam_Open, 
            Amt_Orig_Mfam, 
            SF_Amt_Orig, 
            SB_Amt_Orig, 
            Amt_Orig 
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

def create_loan_distribution_graph(df):
    # Calculate the amount for each loan type
    loan_types = {
        '1-4 Family Closed': df['Amt_Orig_SFam_Closed'].sum(),
        '1-4 Family Open': df['Amt_Orig_SFam_Open'].sum(),
        'Multi-Family': df['Amt_Orig_Mfam'].sum(),
        'Farm Loans': df['SF_Amt_Orig'].sum(),
        'Small Business Loans': df['SB_Amt_Orig'].sum(),
        'Other': df['Amt_Orig'].sum() - df[['Amt_Orig_SFam_Closed', 'Amt_Orig_SFam_Open', 'Amt_Orig_Mfam', 'SF_Amt_Orig', 'SB_Amt_Orig']].sum(axis=1),
        'Total': df['Amt_Orig'].sum()
    }

    # Create a bar chart
    fig = go.Figure(data=[
        go.Bar(name='Loan Type', x=list(loan_types.keys()), y=list(loan_types.values()))
    ])

    # Customize the layout
    fig.update_layout(
        title='Loan Distribution',
        xaxis_title='Loan Type',
        yaxis_title='Amount',
        barmode='group'
    )

    return fig