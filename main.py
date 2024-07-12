import streamlit as st
from modules import CRA_Func as CRA
import polars as pl
from great_tables import GT

st.set_page_config(page_title='CRA Analysis', layout='wide', page_icon=':📊:')

engine = CRA.create_db_connection()
years = ['Select...', '2018', '2019', '2020', '2021']
selected_year = st.selectbox('Select an exam year', options=years)

if selected_year != 'Select...':
    # Create a dropdown menu for bank names
    bank_names = ['Select...'] + sorted(CRA.fetch_bank_names_for_year(engine, selected_year))
    selected_bank = st.selectbox('Select a bank', options=bank_names)

    if selected_bank != 'Select...':
        selected_options = []  # Initialize selected_options to an empty list
        assessment_areas = CRA.fetch_assessment_area(engine, selected_bank, selected_year)

        if assessment_areas is None:
            assessment_areas = {'No assessment areas found': {'codes': ('nan', 'nan', 'nan', 'nan', 'nan'), 'lookup_method': 'nan'}}
            st.write("No assessment areas found for the selected bank and year.")
        else:
            # Add 'Overall' option to the list
            assessment_areas = {'Select...': {'codes': ('nan', 'nan', 'nan', 'nan', 'nan'), 'lookup_method': 'nan'}, **assessment_areas}

            # Create a dropdown menu for assessment areas
            selected_area = st.selectbox('Select an assessment area', options=assessment_areas.keys())
            md_code, msa_code, state_code, county_code, lookup_method = assessment_areas[selected_area]['codes']

            if selected_area != 'Select...':
                options = ['Loan Distribution Graph', 'Loan Distribution Table', 'Assessment Area Distribution Table']
                selected_options = st.multiselect('Select the graphs and tables you want to display:', options)
            
            # Function to create Plotly chart
            def create_plotly_chart():
                df = CRA.fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                figures = CRA.create_loan_distribution_chart(df, selected_area, engine)
                for fig in figures:
                    st.plotly_chart(fig)

            # Function to create Great Tables table
            def create_great_tables_table():
                df = CRA.fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                dataset = CRA.create_loan_distribution_great_tables(df, selected_area, selected_bank)
                if dataset is not None:  # Ensure dataset is not 
                    st.html(dataset.as_raw_html())

            def create_inside_out_table():
                df = CRA.fetch_loan_data_inside_out(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                dataset = CRA.create_inside_out_great_table(df, engine)
                if dataset is not None:  # Ensure dataset is not )
                    st.html(dataset.as_raw_html())


            # Display the selected graphs and tables
            for option in selected_options:
                if option == 'Loan Distribution Graph':
                    create_plotly_chart()
                elif option == 'Loan Distribution Table':
                    st.write(f" {selected_year} Loan Distribution for {selected_bank} in {selected_area}")
                    create_great_tables_table()
                elif option == 'Assessment Area Distribution Table':
                    st.write(f" {selected_year} Assessment Area Distribution Table for {selected_bank} in {selected_area}")
                    create_inside_out_table()
                    