import streamlit as st
from modules import CRA_Func as CRA
from modules import SQL_Queries as SQL

st.set_page_config(page_title='CRA Analysis', layout='wide', page_icon=':📊:')

engine = SQL.create_db_connection()
years = ['Select...', '2018', '2019', '2020', '2021']
selected_year = st.selectbox('Select an exam year', options=years)

if selected_year != 'Select...':
    # Create a dropdown menu for bank names
    bank_names = ['Select...'] + sorted(SQL.fetch_bank_names_for_year(engine, selected_year))
    selected_bank = st.selectbox('Select a bank', options=bank_names)

    if selected_bank != 'Select...':
        # Add radio buttons for "Overall" and "Custom Reports by Region"
        report_type = st.radio('Select report type', ['Overall', 'Custom Reports by Region'])

        if report_type == 'Overall':
            st.write("Placeholder for Overall report page.")
            # Add your function calls or placeholder content for the "Overall" page here
        else:
            selected_options = []  # Initialize selected_options to an empty list
            assessment_areas = SQL.fetch_assessment_area(engine, selected_bank, selected_year)

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
                    options = ['Loan Distribution Graph', 'Loan Distribution Table', 'Assessment Area Distribution Table', 'Borrower Income Table', 'Tract Income Table', 'Business Tract Data', 'Business Size Data', 'Residential Demographics', 'Business Demographics']
                    selected_options = st.multiselect('Select the graphs and tables you want to display:', options)
                    st.markdown(f'<h1 style="font-size:30px;"> CRA Data for {selected_bank} - {selected_year} - {selected_area}</h1>', unsafe_allow_html=True)

                # Function to create Plotly chart
                def create_plotly_chart():
                    df = SQL.fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    figures = CRA.create_loan_distribution_chart(df, selected_area, engine)
                    for fig in figures:
                        st.plotly_chart(fig)

                # Function to create Great Tables table
                def create_great_tables_table():
                    df = SQL.fetch_loan_data_loan_dist(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.create_loan_distribution_great_tables(df, selected_area, selected_bank)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_inside_out_table():
                    df = SQL.fetch_loan_data_inside_out(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.create_inside_out_great_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_bor_income_table():
                    df = SQL.fetch_loan_data_bor_income(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.bor_income_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_tract_income_table():
                    df = SQL.fetch_loan_data_tract_income(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.tract_income_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_tract_business_table():
                    df = SQL.fetch_loan_data_business(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.business_tract_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_business_size_table():
                    df = SQL.fetch_loan_business_size(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.business_size_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_demographics_table():
                    df = SQL.fetch_demographics(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.demographics_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                def create_business_demographics_table():
                    df = SQL.fetch_bus_demographics(engine, selected_bank, selected_year, md_code, msa_code, selected_area, lookup_method, state_code, county_code)
                    dataset = CRA.business_demographics_table(df, selected_bank, selected_area)
                    if dataset is not None:  # Ensure dataset is not None
                        st.html(dataset.as_raw_html())

                # Display the selected graphs and tables
                for option in selected_options:
                    if option == 'Loan Distribution Graph':
                        create_plotly_chart()
                    elif option == 'Loan Distribution Table':
                        create_great_tables_table()
                    elif option == 'Assessment Area Distribution Table':
                        create_inside_out_table()
                    elif option == 'Borrower Income Table':
                        create_bor_income_table()
                    elif option == 'Tract Income Table':
                        create_tract_income_table()
                    elif option == 'Business Tract Data':
                        create_tract_business_table()
                    elif option == 'Business Size Data':
                        create_business_size_table()
                    elif option == 'Residential Demographics':
                        create_demographics_table()
                    elif option == 'Business Demographics':
                        create_business_demographics_table()
