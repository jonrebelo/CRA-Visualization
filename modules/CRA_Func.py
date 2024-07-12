
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from sqlalchemy import create_engine, text
from great_tables import GT, style, loc
import pandas as pd

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


def create_loan_distribution_chart(df, area_name, engine):


    df=df.sum()

    df = df.with_columns([
    (pl.col('Amt_Orig_SFam_Closed') + pl.col('Amt_Orig_SFam_Open') + pl.col('Amt_Orig_MFam') + 
     pl.col('SF_Amt_Orig') + pl.col('SB_Amt_Orig') - pl.col('Amt_Orig')).alias('Business Total')
])
    df = df.with_columns([
        (pl.col('Amt_Orig_SFam_Closed') + pl.col('Amt_Orig_SFam_Open') + pl.col('Amt_Orig_MFam') + 
         pl.col('SF_Amt_Orig') + pl.col('SB_Amt_Orig')).alias('Total Gross Loans')
])

    df_chart = df.drop(['Partial_Ind', 'State_Code', 'County_Code'])

    figures = []

    variable_mapping = {
        'Amt_Orig_SFam_Closed': '1-4 Family Closed-End',
        'Amt_Orig_SFam_Open': '1-4 Family Revolving',
        'Amt_Orig_MFam': 'Multi-Family',
        'SF_Amt_Orig': 'Farm Loans',
        'SB_Amt_Orig': 'Small Business Loans',
        'Amt_Orig': 'Residential Total',
        'Business Total':'Business Total',
        'Total Gross Loans': 'Total Gross Loans'
        
    }


    
    row_data = df_chart.row(0)  # Correctly fetch the single row
    row_data = list(row_data)  # Convert row data to a list for processing
    column_names = [col for col in df_chart.columns]  # Get column names excluding 'label'
    mapped_names = [variable_mapping.get(col, col) for col in column_names]  # Map column names using variable_mapping
    fig = go.Figure(data=[go.Bar(x=mapped_names, y=row_data, name=area_name)])  # Create the figure using the label

    # Add annotations for zero values
    for i, value in enumerate(row_data):
        if value == 0:
            fig.add_annotation(x=mapped_names[i], y=value, text="0", showarrow=False, yshift=10)

    # Set figure layout
    fig.update_layout(
        title_text=f"Loan Distribution for {area_name.rstrip()}",
        yaxis_title="Dollar Amount $(000's)",
        xaxis_title="Loan Type"
    )
    figures.append(fig)  # Append the figure to the list of figures}}}}

    df_partial = df.filter(pl.col('Partial_Ind') == 'Y')

    # Add condition to check if df_partial is not empty
    if df_partial.height > 0:
        total_loan_data = df_partial.sum().drop(['Partial_Ind', 'State_Code', 'County_Code', 'label'])

        total_loan_data = total_loan_data.with_columns([
            (pl.col('Amt_Orig_SFam_Closed') + pl.col('Amt_Orig_SFam_Open') +
            pl.col('Amt_Orig_MFam') + pl.col('SF_Amt_Orig') + pl.col('SB_Amt_Orig')).alias('Total Gross Loans')
        ])

        total_loan_data_list = [total_loan_data[col][0] for col in total_loan_data.columns]
        mapped_total_names = [variable_mapping.get(col, col) for col in total_loan_data.columns]
        fig = go.Figure(data=[go.Bar(x=mapped_total_names, y=total_loan_data_list, name="Total")])
        # Add annotations for zero values
        for i, value in enumerate(total_loan_data_list):
            if value == 0:
                fig.add_annotation(x=mapped_total_names[i], y=value, text="0", showarrow=False, yshift=10)
        fig.update_layout(
            title_text="Total Loan Distribution",
            yaxis_title="Dollar Amount $(000's)",
            xaxis_title="Loan Type"
        )
        figures.append(fig)

    return figures




def create_loan_distribution_great_tables(df, area_name, selected_bank):
    df = df.sum()

    df = df.with_columns([
        (pl.col('Amt_Orig_SFam_Closed') + pl.col('Amt_Orig_SFam_Open') + pl.col('Amt_Orig_MFam') + 
     pl.col('SF_Amt_Orig') + pl.col('SB_Amt_Orig') - pl.col('Amt_Orig')).alias('Business Total'),
    (pl.col('Amt_Orig_SFam_Closed') + pl.col('Amt_Orig_SFam_Open') + pl.col('Amt_Orig_MFam') + 
     pl.col('SF_Amt_Orig') + pl.col('SB_Amt_Orig')).alias('Total Gross Loans')

])  
    
    

    # Ensure columns are correctly typed (if necessary)
    df = df.cast(
        {'Amt_Orig_SFam_Closed': pl.Int64,
         'Amt_Orig_SFam_Open': pl.Int64,
         'Amt_Orig_MFam': pl.Int64,
         'SF_Amt_Orig': pl.Int64,
         'SB_Amt_Orig': pl.Int64,
         'Amt_Orig': pl.Int64,
         'Business Total': pl.Int64,
         'Total Gross Loans': pl.Int64}
    )

    df = df.rename(
    {
        'Amt_Orig_SFam_Closed': '1-4 Family Closed-End',
        'Amt_Orig_SFam_Open': '1-4 Family Revolving',
        'Amt_Orig_MFam': 'Multi-Family',
        'SF_Amt_Orig': 'Farm Loans',
        'SB_Amt_Orig': 'Small Business Loans',
        'Amt_Orig': 'Residential Total',
    }
    
)
    
    df = df.drop(['Partial_Ind', 'State_Code', 'County_Code'])
    df = df.transpose(include_header = True)
    print(df)

    total_gross_loans = df.item(7,1)

    if total_gross_loans is not None:
        df = df.with_columns(
        (pl.col('column_0').map_elements(lambda x: x / total_gross_loans, return_dtype=pl.Float64)).alias('Loan Percentages')
        )

    # Create Great Tables instance with Polars DataFrame
    gt_instance = (
    GT(df)
    .opt_table_outline()
    .opt_stylize(style = 2, color = "blue")
    .tab_header(title = "Loan Distribution", subtitle = f"{selected_bank} in {area_name.rstrip()}")
    .cols_label(column = 'Loan Type', column_0 = "Amount $(000's)" )
    .fmt_percent(columns = 'Loan Percentages', decimals = 1)
    .fmt_number(columns= "column_0", use_seps = True, decimals = 0)
    .tab_style(
        style=style.text(style = "italic",),
        locations=loc.body(rows=[5, 6]),
    )
    .tab_style(
        style=style.fill(color="lightyellow"),
        locations=loc.body(rows=[5, 6]),
    )
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[7]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[7]),
    )
    .tab_options(
    table_body_hlines_style="solid",
    table_body_vlines_style="solid",
    table_body_border_top_color="gray",
    table_body_border_bottom_color="gray",
    container_width = "100%"   
    )
    )

    # Return Great Tables instance
    return gt_instance


def fetch_loan_data_inside_out(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code):

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




def create_inside_out_great_table(df, engine):

    df = df.sum()
# Create a new dataframe with the specified rows and columns
    data = {
        'Loan Type': ['Single Family', 'Revolving', 'Multi Family'],
        '#': [df['Loan_Orig_SFam_Closed_Inside'].to_numpy()[0], df['Loan_Orig_SFam_Open_Inside'].to_numpy()[0], df['Loan_Orig_MFam_Inside'].to_numpy()[0]],
        '000s': [df['Amt_Orig_SFam_Closed_Inside'].to_numpy()[0], df['Amt_Orig_SFam_Open_Inside'].to_numpy()[0], df['Amt_Orig_MFam_Inside'].to_numpy()[0]],
        '#2': [df['Loan_Orig_SFam_Closed'].to_numpy()[0] - df['Loan_Orig_SFam_Closed_Inside'].to_numpy()[0], df['Loan_Orig_SFam_Open'].to_numpy()[0] - df['Loan_Orig_SFam_Open_Inside'].to_numpy()[0], df['Loan_Orig_MFam'].to_numpy()[0] - df['Loan_Orig_MFam_Inside'].to_numpy()[0]],
        '000s2': [df['Amt_Orig_SFam_Closed'].to_numpy()[0] - df['Amt_Orig_SFam_Closed_Inside'].to_numpy()[0], df['Amt_Orig_SFam_Open'].to_numpy()[0] - df['Amt_Orig_SFam_Open_Inside'].to_numpy()[0], df['Amt_Orig_MFam'].to_numpy()[0] - df['Amt_Orig_MFam_Inside'].to_numpy()[0]],
        '# Total': [df['Loan_Orig_SFam_Closed'].to_numpy()[0], df['Loan_Orig_SFam_Open'].to_numpy()[0], df['Loan_Orig_MFam'].to_numpy()[0]],
        '000s Total': [df['Amt_Orig_SFam_Closed'].to_numpy()[0], df['Amt_Orig_SFam_Open'].to_numpy()[0], df['Amt_Orig_MFam'].to_numpy()[0]]
    }
    new_df = pd.DataFrame(data)

    # Add 'Residential Total' row
    residential_total = pd.DataFrame({'Loan Type': ['Residential Total'], '#': [new_df['#'].sum()], '000s': [new_df['000s'].sum()], '#2': [new_df['# Total'].sum() - new_df['#'].sum()], '000s2': [new_df['000s Total'].sum() - new_df['000s'].sum()], '# Total': [new_df['# Total'].sum()], '000s Total': [new_df['000s Total'].sum()]})
    new_df = pd.concat([new_df, residential_total])

    # Add 'Small Business' and 'Farm' rows
    business_data = {
    'Loan Type': ['Small Business', 'Farm'],
    '#': [df['SB_Loan_Orig_Inside'].to_numpy()[0], df['SF_Loan_Orig_Inside'].to_numpy()[0]],
    '000s': [df['SB_Amt_Orig_Inside'].to_numpy()[0], df['SF_Amt_Orig_Inside'].to_numpy()[0]],
    '#2': [df['SB_Loan_Orig'].to_numpy()[0] - df['SB_Loan_Orig_Inside'].to_numpy()[0], df['SF_Loan_Orig'].to_numpy()[0] - df['SF_Loan_Orig_Inside'].to_numpy()[0]],
    '000s2': [df['SB_Amt_Orig'].to_numpy()[0] - df['SB_Amt_Orig_Inside'].to_numpy()[0], df['SF_Amt_Orig'].to_numpy()[0] - df['SF_Amt_Orig_Inside'].to_numpy()[0]],
    '# Total': [df['SB_Loan_Orig'].to_numpy()[0], df['SF_Loan_Orig'].to_numpy()[0]],
    '000s Total': [df['SB_Amt_Orig'].to_numpy()[0], df['SF_Amt_Orig'].to_numpy()[0]]
}
    business_df = pd.DataFrame(business_data)
    new_df = pd.concat([new_df, business_df])

    # Add 'Business Total' and 'Grand Total' rows
    business_total = pd.DataFrame({'Loan Type': ['Business Total'], '#': [new_df['#'][3:].sum()], '000s': [new_df['000s'][3:].sum()], '#2': [new_df['# Total'][3:].sum() - new_df['#'][3:].sum()], '000s2': [new_df['000s Total'][3:].sum() - new_df['000s'][3:].sum()], '# Total': [new_df['# Total'][3:].sum()], '000s Total': [new_df['000s Total'][3:].sum()]})
    grand_total = pd.DataFrame({'Loan Type': ['Grand Total'], '#': [new_df['#'].sum()], '000s': [new_df['000s'].sum()], '#2': [new_df['# Total'].sum() - new_df['#'].sum()], '000s2': [new_df['000s Total'].sum() - new_df['000s'].sum()], '# Total': [new_df['# Total'].sum()], '000s Total': [new_df['000s Total'].sum()]})
    new_df = pd.concat([new_df, business_total, grand_total])

    new_df['#%'] = new_df['#'] / new_df['# Total']
    new_df['000s%'] = new_df['000s'] / new_df['000s Total']
    new_df['#2%'] = new_df['#2'] / new_df['# Total']
    new_df['000s2%'] = new_df['000s2'] / new_df['000s Total']
    new_df['#Total%'] = new_df['#%'] + new_df['#2%']
    new_df['000s Total %'] = new_df['000s%'] + new_df['000s2%']
    new_df.fillna(0, inplace=True)

    new_df.columns = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'
    ]

    # Create a Great Tables instance with the new dataframe
    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style=2, color="blue")
    .tab_style(
        style=style.text(style = "italic",),
        locations=loc.body(rows=[3, 6]),
    )
    .tab_style(
        style=style.fill(color="lightyellow"),
        locations=loc.body(rows=[3, 6]),
    )
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[7]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[7]),
    )
    .tab_header("Assessment Area Distribution")
    .tab_spanner(label="Inside", columns=['b', 'h', 'c', 'i'])
    .tab_spanner(label="Outside", columns=['d', 'j', 'e', 'k'])
    .tab_spanner(label="Totals", columns=['f', 'l', 'g', 'm'])
    .fmt_percent(columns=['h', 'i', 'j', 'k', 'l', 'm'], decimals=1)
    .fmt_number(columns=[ 'b', 'c', 'd', 'e', 'f', 'g' ], decimals=0, use_seps=True)
    .cols_label(a = "Loan Type",
                b = "#",
                c = '000s',
                d = "#",
                e = "000s",
                f = "#",
                g = '000s',
                h = "%",
                i = '%',
                j = '%',
                k = '%',
                l = '%',
                m = '%')
    .tab_options(
        table_body_hlines_style="solid",
        table_body_vlines_style="solid",
        table_body_border_top_color="gray",
        table_body_border_bottom_color="gray",
        container_width="100%"
    )
    .opt_vertical_padding(scale=1.5)
    .opt_horizontal_padding(scale=1.2)
    
)
    return gt_instance