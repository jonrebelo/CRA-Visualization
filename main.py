import streamlit as st
from st_aggrid import AgGrid
from modules import CRA_Func as CRA
import pandas as pd

st.set_page_config(page_title='CRA Analysis', layout='wide', page_icon=':🎰:')

# Initialize session_state if it doesn't exist
if 'page' not in st.session_state:
    st.session_state['page'] = 'Home'

# Load the data
df = pd.read_csv('retail_loan_hmda_bank_total_2021.csv')

# Create a list of unique banks
banks = df['id_rssd'].unique().tolist()

# Create a dropdown for selecting the bank
selected_bank = st.sidebar.selectbox('Select a bank', banks)


if st.session_state['page'] == 'Home':
    # Define the options for the multi-select dropdown menu
    options = ['Loan Amount Data Graph', 'Loan Originated Data Graph', 'Loans by Location Graph', 'Loan Data Table', 'Census Income Data Table', 'Borrower Income Data Table', 'Combined Income Data Table']
    selected_options = st.sidebar.multiselect('Select the graphs and tables you want to display:', options)
    # Modify the function mappings to pass the benchmark to the functions
    graph_functions = {
        
                'Loan Amount Data Graph': lambda df, bank: CRA.plot_loan_amt_data(df, bank),
                'Loan Originated Data Graph': lambda df, bank: CRA.plot_loan_orig_data(df, bank),
                'Loans by Location Graph': lambda df, bank: CRA.plot_loans_by_location(df, bank),
    }
    table_functions = {
                'Loan Data Table': lambda df, bank: CRA.calculate_loan_data(df, bank),
                'Census Income Data Table': lambda df, bank: CRA.calculate_census_income_data(df, bank),
                'Borrower Income Data Table': lambda df, bank: CRA.calculate_bor_income_data(df, bank),
                'Combined Income Data Table': lambda df, bank: CRA.calculate_combined_income_data(df, bank),
            }

        # Initialize a list to keep track of which column is free
    columns = []
    free_column_index = 0
        # Initialize a dictionary to store tables
    tables = {}
        # Initialize a dictionary to store graphs
    graphs = {}

    for option in selected_options:
            # If the option is 'Monthly Heatmap Graph', display it in a full-width container
            if option == 'Monthly Heatmap Graph':
                fig = graph_functions[option](selected_bank)
                if fig is not None:
                    st.plotly_chart(fig)
                    # Convert the figure to HTML and store it in the dictionary
                    graph_html = fig.to_html(full_html=False)
                    graphs[option] = graph_html
                continue  # Skip the rest of the loop

            # Always create 2 columns
            if free_column_index == 0:
                columns = st.columns(2)

            # Generate and display the graph or table
            if option in graph_functions:
                fig = graph_functions[option](df, selected_bank)
                if fig is not None:
                    columns[free_column_index].plotly_chart(fig)
                    # Convert the figure to HTML and store it in the dictionary
                    graph_html = fig.to_html(full_html=False)
                    graphs[option] = graph_html
            elif option in table_functions:
                df_result = table_functions[option](df, selected_bank)
                if df_result is not None:
                    # Generate column definitions based on DataFrame columns
                    column_defs = [{'headerName': col, 'field': col, 'filter': True} for col in df_result.columns]

                    gridOptions={
                        'columnDefs': column_defs,
                        'defaultColDef': {'flex': 1, 'editable': False},
                        'fit_columns_on_grid_load': True,
                    }
                    with columns[free_column_index].container():
                        st.markdown(f"###### **{option}**")  # Add a title to the table
                        AgGrid(df_result, gridOptions=gridOptions)

                    # Store the table in the dictionary
                    tables[option] = df_result

                # Switch to the next column for the next figure or table
            free_column_index = (free_column_index + 1) % 2
