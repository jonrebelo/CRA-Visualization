import streamlit as st
from st_aggrid import AgGrid
from modules import CRA_Func as CRA
import pandas as pd
import plotly.express as px

st.set_page_config(page_title='CRA Analysis', layout='wide', page_icon=':🎰:')

# Initialize session_state if it doesn't exist
if 'page' not in st.session_state:
    st.session_state['page'] = 'Home'

@st.cache_data
def fetch_data():
    df = pd.read_csv('data/retail_loan_hmda_bank_total_2021.csv')
    df_inside = pd.read_csv('data/retail_loan_hmda_bank_inside_2021.csv')
    return df, df_inside

@st.cache_data
def load_bank_mapping(file_path):
    # Load filtered_institutions.csv
    df_mapping = pd.read_csv(file_path)
    df_mapping.columns = df_mapping.columns.str.strip()
    
    # Create a dictionary mapping id_rssd to bank names
    id_rssd_to_bank = {row['FED_RSSD']: row['NAME'] for _, row in df_mapping.iterrows()}
    
    return id_rssd_to_bank

# Fetch data
df, df_inside = fetch_data()

# Use institutions.csv as the source for bank names
filtered_institutions_file = 'data/filtered_institutions_final.csv'
id_rssd_to_bank = load_bank_mapping(filtered_institutions_file)

# Create a list of unique banks using bank names
banks = list(id_rssd_to_bank.values())
banks.sort()

# Filter the id_rssd_to_bank dictionary to only include banks that are present in the df DataFrame
id_rssd_to_bank = {id_rssd: bank for id_rssd, bank in id_rssd_to_bank.items() if id_rssd in df['id_rssd'].unique()}

# Create a dropdown for selecting the bank
selected_bank_name = st.sidebar.selectbox('Select an Institution', banks)

# Get the corresponding id_rssd from the bank name selected
selected_id_rssd = next(id_rssd for id_rssd, bank_name in id_rssd_to_bank.items() if bank_name == selected_bank_name)

if st.session_state['page'] == 'Home':
    options = ['Loan Amount Data Graph', 'Loan Originated Data Graph', 'Loans by Location Graph', 'Loan Data Table', 'Census Income Data Table', 'Borrower Income Data Table', 'Combined Income Data Table', 'Loans Originated in Assessment Area']
    selected_options = st.sidebar.multiselect('Select the graphs and tables you want to display:', options)
    
    graph_functions = {
        'Loan Amount Data Graph': lambda df, bank: CRA.plot_loan_amt_data(df, bank),
        'Loan Originated Data Graph': lambda df, bank: CRA.plot_loan_orig_data(df, bank),
        'Loans by Location Graph': lambda df, bank: CRA.plot_loans_by_location(df, bank),
        'Loans Originated in Assessment Area': lambda df, df_inside, bank: CRA.plot_loan_orig_comparison(df, df_inside, bank)
    }
    
    table_functions = {
        'Loan Data Table': lambda df, bank: CRA.calculate_loan_data(df, bank),
        'Census Income Data Table': lambda df, bank: CRA.calculate_census_income_data(df, bank),
        'Borrower Income Data Table': lambda df, bank: CRA.calculate_bor_income_data(df, bank),
        'Combined Income Data Table': lambda df, bank: CRA.calculate_combined_income_data(df, bank),
    }

    columns = []
    free_column_index = 0
    tables = {}
    graphs = {}

    for option in selected_options:
        if option == 'Monthly Heatmap Graph':
            fig = graph_functions[option](selected_id_rssd)
            if fig is not None:
                st.plotly_chart(fig)
                graph_html = fig.to_html(full_html=False)
                graphs[option] = graph_html
            continue

        if free_column_index == 0:
            columns = st.columns(2)

        if option in graph_functions:
            if option == 'Loans Originated in Assessment Area':
                fig = graph_functions[option](df, df_inside, selected_id_rssd)
            else:
                fig = graph_functions[option](df, selected_id_rssd)
            if fig is not None:
                columns[free_column_index].plotly_chart(fig)
                graph_html = fig.to_html(full_html=False)
                graphs[option] = graph_html
        elif option in table_functions:
            df_result = table_functions[option](df, selected_id_rssd)
            if df_result is not None:
                column_defs = [{'headerName': col, 'field': col, 'filter': True} for col in df_result.columns]
                gridOptions = {
                    'columnDefs': column_defs,
                    'defaultColDef': {'flex': 1, 'editable': False},
                    'fit_columns_on_grid_load': True,
                }
                with columns[free_column_index].container():
                    st.markdown(f"###### **{option}**")
                    AgGrid(df_result, gridOptions=gridOptions)
                tables[option] = df_result

        free_column_index = (free_column_index + 1) % 2