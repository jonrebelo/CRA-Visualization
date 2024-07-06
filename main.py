import streamlit as st
from st_aggrid import AgGrid
from modules import CRA_Func as CRA

st.set_page_config(page_title='CRA Analysis', layout='wide', page_icon=':📊:')

# Initialize the session state if it doesn't exist
if 'page' not in st.session_state:
    st.session_state['page'] = 'Home'

# Create a navigation menu
page = st.sidebar.selectbox("Navigation", ['Home', 'Other Page'])

if page != st.session_state['page']:
    st.session_state['page'] = page

if st.session_state['page'] == 'Home':
    engine = CRA.create_db_connection()

    # Create a dropdown menu for bank names
    selected_bank = st.selectbox(
        'Select a bank',
        options=sorted(CRA.fetch_bank_names(engine))
    )

    # Create a dropdown menu for exam years
    selected_year = st.selectbox(
        'Select an exam year',
        options=sorted(CRA.fetch_years_for_bank(engine, selected_bank))
    )

    assessment_areas = CRA.fetch_assessment_area(engine, selected_bank, selected_year)
    if assessment_areas is None:
        assessment_areas = {'No assessment areas found': {'codes': ('nan', 'nan', 'nan', 'nan', 'nan'), 'lookup_method': 'nan'}}
        st.write("No assessment areas found for the selected bank and year.")
    else:
        # Add 'Overall' option to the list
        assessment_areas = {'Overall': {'codes': ('nan', 'nan', 'nan', 'nan', 'nan'), 'lookup_method': 'nan'}, **assessment_areas}

        # Create a dropdown menu for assessment areas
        selected_area = st.selectbox(
            'Select an assessment area',
            options=assessment_areas.keys()
        )
        md_code, msa_code, state_code, county_code, lookup_method = assessment_areas[selected_area]['codes']

# Add code for 'Other Page' here
elif st.session_state['page'] == 'Other Page':
    st.write("This is another page.")

if st.session_state['page'] == 'Home':
    options = ['Loan Distribution']
    selected_options = st.sidebar.multiselect('Select the graphs and tables you want to display:', options)
    
    # Fetch the loan data
    df = CRA.fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)

    graph_functions = {
        'Loan Distribution': lambda: CRA.create_loan_distribution_graph(df)
    }

    # Display the selected graphs
    for option in selected_options:
        if option in graph_functions:
            fig = graph_functions[option]()
            st.plotly_chart(fig)