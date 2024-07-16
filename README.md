# CRA Analysis Application

This application is designed to parse and analyze information regarding Community Reinvestment Act (CRA) data provided by the Federal Reserve. The data can be found at [Federal Reserve CRA Data](https://www.federalreserve.gov/consumerscommunities/data_tables.htm).

## Required Packages

The application requires the following Python packages:

- streamlit
- great_tables
- polars
- plotly
- sqlalchemy
- pandas

You can install these packages using pip:

```bash
pip install streamlit polars great_tables plotly sqlalchemy pandas
```

### Functionality
The application is built with Streamlit and provides an interactive interface for analyzing CRA data. It allows users to select a bank from a dropdown menu and then choose which graphs and tables to display from a multi-select dropdown menu. The options include:

- Loan Amount Data Graph
- Loan Originated Data Graph
- Loans by Location Graph
- Loan Data Table
- Census Income Data Table
- Borrower Income Data Table
- Combined Income Data Table
- & More selectable within the searchable dropdown.
-The selected graphs and tables are displayed in the Streamlit application. The graphs are created using Plotly and the tables are displayed using Ag-Grid.

![Screenshot 1](/screenshots/Screenshot.png)
![Screenshot 2](/screenshots/Screenshot2.png)

#### Future Development
This application is a rough first draft of a tool that could evolve into a great resource for evaluating CRA data provided by the Federal Reserve. It's still under active development and there are plans to add more tables and graphs, along with the ability to export data.

##### Usage

Download the data from https://www.federalreserve.gov/consumerscommunities/data_tables.htm. 

retail_loan_lending_test.csv and performance_evaluation_table.csv were used for this application.

Download MSA Tract data from https://www.ffiec.gov/. Click on MSA/MD state county tract crosswalk  to get the xlsx.

Put both these files in the data folder.

To run the application, navigate to main.py and in the command line enter "streamlit run" followed by the filepath of main.py. This will open a web page with the application in it.

Find the charter ID of the bank in question and enter it into the search box, then select the graphs and tables you wish to view.

###### Contribution
Contributions are welcome! Please feel free to submit a pull request or open an issue if you have any suggestions or find any bugs.
