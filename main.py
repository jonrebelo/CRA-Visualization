import streamlit as st
from modules import CRA_Func as CRA
from modules import SQL_Queries as SQL
from modules import Format as fmt


st.set_page_config(page_title='CRA Analysis', layout='wide', page_icon=':📊:')

engine = SQL.create_db_connection()
years = ['Select...', '2018', '2019', '2020', '2021']
selected_year = st.selectbox('Select an exam year', options=years)



if selected_year != 'Select...':
    # Create a dropdown menu for bank names
    bank_names = ['Select...'] + sorted(SQL.fetch_bank_names_for_year(engine, selected_year))
    selected_bank = st.selectbox('Select an Institution', options=bank_names)

    if selected_bank != 'Select...':
        # Add radio buttons for "Overall" and "Custom Reports by Region"
        report_type = st.radio('Select report type', ['Overall', 'Custom Reports by Region'])

        if report_type == 'Overall':
            # Fetch data
            df = SQL.fetch_loan_data_overall(engine, selected_bank, selected_year)
            
            # Summarize data
            first_row = df.head(1).to_dict(as_series=True)
            bank_county_deposits = df['bank_county_deposits'].sum()
            number_of_branches = df['number_of_branches'].sum()
            
            id_rssd = first_row['id_rssd'][0]
            hmda_assets = fmt.format_number(first_row['hmda_assets'][0])
            cra_current_year_assets = fmt.format_number(first_row['cra_current_year_assets'][0])
            cra_previous_year_assets = fmt.format_number(first_row['cra_previous_year_assets'][0])
            lender_in_cra = first_row['Lender_in_CRA'][0]
            
            # Format the summed deposits
            bank_county_deposits = fmt.format_number(bank_county_deposits)
            
            if lender_in_cra == "Y":
                summary = f"""
                <style>
                    .summary-header {{
                        font-size: 2em;
                        font-weight: bold;
                        text-align: center;
                        margin-bottom: 20px;
                    }}
                    .summary-subheader {{
                        font-size: 1.5em;
                        font-weight: bold;
                        text-align: center;
                        margin-bottom: 10px;
                    }}
                    .summary-body {{
                        font-size: 1.2em;
                        margin: 10px;
                    }}
                    .summary-bold {{
                        font-weight: bold;
                        color: #333;
                    }}
                    .summary-highlight {{
                        color: #007bff;
                    }}
                    .summary-key {{
                        font-weight: bold;
                        font-size: 1.2em;
                    }}
                    .summary-value {{
                        font-size: 1.2em;
                    }}
                    .summary-value-small {{
                        font-size: 1em;
                    }}
                    .spacer {{
                        margin: 20px 0; /* Adjust the spacing here */
                    }}
                    .header {{
                        text-align: center;
                        margin-top: 30px; /* Space above the header */
                        margin-bottom: 20px; /* Space below the header */
                    }}
                    .section-header {{
                        font-size: 1.8em;
                        font-weight: bold;
                        margin-top: 40px; /* Space above the section header */
                        margin-bottom: 20px; /* Space below the section header */
                    }}
                    .disclaimer {{
                        font-size: 0.9em;
                        margin-top: 40px;
                        border-top: 1px solid #ddd;
                        padding-top: 10px;
                        text-align: center;
                        color: #555;
                    }}
                </style>
                
                <div class="summary-header">Summarized CRA Report</div>
                <div class="summary-subheader"><span class="summary-highlight">{selected_bank}</span></div>
                <div class="summary-body">
                    <p><span class="summary-key">Year:</span> <span class="summary-value">{selected_year}</span></p>
                    <p><span class="summary-key">National ID:</span> <span class="summary-value">{id_rssd}</span></p>
                    <p class="spacer"></p> <!-- Added spacing using CSS -->
                    <p><span class="summary-bold">{selected_bank}</span> reported <span class="summary-bold">{number_of_branches}</span> total branches within its lending areas, holding <span class="summary-bold">${bank_county_deposits}</span> in total deposits as of <span class="summary-value-small">{selected_year}</span>.</p>
                    <p><span class="summary-bold">${hmda_assets}</span> in HMDA reported assets for the <span class="summary-value-small">{selected_year}</span>.</p>
                    <p><span class="summary-bold">${cra_current_year_assets}</span> in CRA reported assets for the <span class="summary-value-small">{selected_year}</span>, and <span class="summary-bold">${cra_previous_year_assets}</span> for the year prior.</p>
                </div>
                """
            else:
                summary = f"""
                <style>
                    .summary-header {{
                        font-size: 2em;
                        font-weight: bold;
                        text-align: center;
                        margin-bottom: 20px;
                    }}
                    .summary-subheader {{
                        font-size: 1.5em;
                        font-weight: bold;
                        text-align: center;
                        margin-bottom: 10px;
                    }}
                    .summary-body {{
                        font-size: 1.2em;
                        margin: 10px;
                    }}
                    .summary-bold {{
                        font-weight: bold;
                        color: #333;
                    }}
                    .summary-highlight {{
                        color: #007bff;
                    }}
                    .summary-key {{
                        font-weight: bold;
                        font-size: 1.2em;
                    }}
                    .summary-value {{
                        font-size: 1.2em;
                    }}
                    .summary-value-small {{
                        font-size: 1em;
                    }}
                    .spacer {{
                        margin: 20px 0; /* Adjust the spacing here */
                    }}
                    .header {{
                        text-align: center;
                        margin-top: 30px; /* Space above the header */
                        margin-bottom: 20px; /* Space below the header */
                    }}
                    .section-header {{
                        font-size: 1.8em;
                        font-weight: bold;
                        margin-top: 40px; /* Space above the section header */
                        margin-bottom: 20px; /* Space below the section header */
                    }}
                    .disclaimer {{
                        font-size: 0.9em;
                        margin-top: 40px;
                        border-top: 1px solid #ddd;
                        padding-top: 10px;
                        text-align: center;
                        color: #555;
                    }}
                </style>
                
                <div class="summary-header">Summarized CRA Report</div>
                <div class="summary-subheader"><span class="summary-highlight">{selected_bank}</span></div>
                <div class="summary-body">
                    <p><span class="summary-key">Year:</span> <span class="summary-value">{selected_year}</span></p>
                    <p><span class="summary-key">National ID:</span> <span class="summary-value">{id_rssd}</span></p>
                    <p class="spacer"></p> <!-- Added spacing using CSS -->
                    <p><span class="summary-bold">{selected_bank}</span> reported <span class="summary-bold">{number_of_branches}</span> total branches within its lending areas, holding <span class="summary-bold">${bank_county_deposits}</span> in total deposits as of <span class="summary-value-small">{selected_year}</span>.</p>
                    <p><span class="summary-bold">${hmda_assets}</span> in HMDA reported assets for the <span class="summary-value-small">{selected_year}</span>.</p>
                    <p>{selected_bank} was not a CRA reporter in <span class="summary-value-small">{selected_year}</span>.</p>
                </div>
                """
            
            st.markdown(summary, unsafe_allow_html=True)
            
            # Add headers above the columns
            st.markdown('<div class="header"><h2>Summarized Lending Splits</h2></div>', unsafe_allow_html=True)
            
            # Display charts with adjusted column widths
            loan_dist = CRA.overall_distribution_great_tables(df, selected_bank)
            inside_out = CRA.overall_inside_out_great_table(df, selected_bank)
            
            col1, col2 = st.columns([1, 2])  # col1 is 33.3% and col2 is 66.7% of the width
            with col1:
                st.html(loan_dist.as_raw_html())
            with col2:
                st.html(inside_out.as_raw_html())
            
            st.markdown('<div class="header"><h2>Lending Statistics by Loan Type</h2></div>', unsafe_allow_html=True)
            
            overall_areas = assessment_areas = SQL.fetch_assessment_area(engine, selected_bank, selected_year)
            top_areas = CRA.top_areas(engine, df, selected_bank, selected_year, overall_areas)
            top_bus = CRA.top_business_areas(engine, df, selected_bank, selected_year, overall_areas)
            top_farm = CRA.top_farm_areas(engine, df, selected_bank, selected_year, overall_areas)
            
            col3, col4, col5 = st.columns(3)
            with col3:
                st.html(top_areas.as_raw_html())
            with col4:
                st.html(top_bus.as_raw_html())
            with col5:
                st.html(top_farm.as_raw_html())

            # Disclaimer
            st.markdown("""
            <div class="disclaimer">
                <p><strong>Disclaimer:</strong></p>
                <p>The banking and financial data used in this report is sourced from the <a href="https://www.federalreserve.gov/consumerscommunities/data_tables.htm" target="_blank">Federal Reserve</a>.</p>
                <p>Geographical data is sourced from the <a href="https://www.ffiec.gov/" target="_blank">FFIEC</a>.</p>
                <p>There may be slight deviations in the data due to different fiscal year timings and the fact that our data is separated by activity within the calendar year.</p>
                <p>While every effort is made to ensure accuracy, please verify any critical information with official sources or consult a financial expert.</p>
            </div>
            """, unsafe_allow_html=True)

            # Generate HTML for download
            html_content = fmt.generate_html_summary(summary)
            
            # Display the download link
            st.markdown(fmt.download_link(html_content, "CRA_Report.html"), unsafe_allow_html=True)

        else:
            selected_options = []  # Initialize selected_options to an empty list
            assessment_areas = SQL.fetch_assessment_area(engine, selected_bank, selected_year)

            if assessment_areas is None:
                assessment_areas = {'No assessment areas found': {'codes': ('nan', 'nan', 'nan', 'nan', 'nan'), 'lookup_method': 'nan'}}
                st.write("No assessment areas found for the selected bank and year.")
            else:
                sorted_areas = fmt.group_and_sort_assessment_areas(assessment_areas.keys())

                # Create a dropdown menu for assessment areas, default to the first item
                if sorted_areas:
                    selected_area = st.selectbox('Select an assessment area', options=sorted_areas, index=0)
                    md_code, msa_code, state_code, county_code, lookup_method = assessment_areas[selected_area]['codes']

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
