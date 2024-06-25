import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def fetch_data():
    df= pd.read_csv('data/retail_loan_hmda_bank_total_2021.csv')
    df_inside = pd.read_csv('data/retail_loan_hmda_bank_inside_2021.csv')
    return df, df_inside

df = fetch_data()

def fetch_data():
    df = pd.read_csv('data/retail_loan_hmda_bank_total_2021.csv')
    df_inside = pd.read_csv('data/retail_loan_hmda_bank_inside_2021.csv')
    return df, df_inside

def plot_loan_orig_comparison(df, df_inside, id_rssd):
    # Filter the DataFrames based on the id_rssd
    df_total_filtered = df[df['id_rssd'] == id_rssd]
    df_inside_filtered = df_inside[df_inside['id_rssd'] == id_rssd]

    # Check if id_rssd is found in df_inside and print the result
    if not df_inside_filtered.empty:
        total_loan_orig_inside = df_inside_filtered['Loan_Orig_Inside'].sum()
        print(f"id_rssd {id_rssd} found in df_inside with Loan_Orig_Inside: {total_loan_orig_inside}")
    else:
        total_loan_orig_inside = 0
        print(f"id_rssd {id_rssd} not found in df_inside")

    # Aggregate the total Loan_Orig
    total_loan_orig = df_total_filtered['Loan_Orig'].sum()

    # Create a DataFrame for plotting
    comparison_df = pd.DataFrame({
        'Category': ['Total Loan Originated', 'Loan Originated Inside'],
        'Count': [total_loan_orig, total_loan_orig_inside]
    })

    # Create the bar graph
    fig = px.bar(comparison_df, x='Category', y='Count', title='Comparison of Loan Originations')

    # Update the labels of the axes
    fig.update_xaxes(title_text='Category')
    fig.update_yaxes(title_text='Count of Loans Originated')

    return fig

def get_bank_info(id_rssd):
    pd.set_option('display.max_rows', None)  # display all rows
    bank_data = df[df['id_rssd'] == id_rssd]
    bank_data = bank_data.loc[:, (bank_data != 0).any(axis=0)]
    return bank_data


def plot_loans_by_location(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    # Create a new column that combines the 'State_Code' and 'County_Code' columns
    df['Location'] = df['State_Code'].astype(str) + '/' + df['County_Code'].astype(str)

    # Group by the new column and sum the 'Loan_Orig' column
    location_loan_orig = df.groupby('Location')['Loan_Orig'].sum().reset_index()

    # Sort the data by 'Loan_Orig' in descending order
    location_loan_orig = location_loan_orig.sort_values('Loan_Orig', ascending=False)

    # Calculate and print the total loans originated
    total_loans_originated = location_loan_orig['Loan_Orig'].sum()
    print(f"Total Loans Originated: {total_loans_originated}")

    # Create the bar graph
    fig = px.bar(location_loan_orig, x='Location', y='Loan_Orig', title='Purchase Loans Originated by Location')

    # Update the labels of the axes
    fig.update_xaxes(title_text='Census Tract')
    fig.update_yaxes(title_text='Loans Originated')

    return fig

def calculate_loan_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    # Create a new DataFrame with 'Loan Type' and 'Amounts' columns
    loan_types = ['1-4 Family', 'Multifamily']
    amount_columns = ['Amt_Orig_SFam', 'Amt_Orig_MFam']
    amounts = [df[col].sum() for col in amount_columns]
    data = { 'Loan Type': loan_types, 'Amounts': amounts }
    loan_data = pd.DataFrame(data)

    # Add a third column for percentages
    total = loan_data['Amounts'].sum()
    loan_data['Percentage'] = (loan_data['Amounts'] / total * 100).round(2)

    # Add a third row for totals
    totals = {'Loan Type': 'Total', 'Amounts': total, 'Percentage': 100.0}
    loan_data = loan_data.append(totals, ignore_index=True)

    return loan_data


def calculate_census_income_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    # Define the categories and corresponding columns
    income_categories = ['Low', 'Moderate', 'Middle', 'Upper', 'Unknown']
    loan_orig_columns = ['Loan_Orig_TILow','Loan_Orig_TIMod', 'Loan_Orig_TIMid','Loan_Orig_TIUpp','Loan_Orig_TIUnk']
    loan_amt_columns = ['Amt_Orig_TILow', 'Amt_Orig_TIMod', 'Amt_Orig_TIMid', 'Amt_Orig_TIUpp', 'Amt_Orig_TIUnk']

    # Calculate the sums for each category
    loan_orig_sums = [df[col].sum() for col in loan_orig_columns]
    loan_amt_sums = [df[col].sum() for col in loan_amt_columns]

    # Calculate the total sums
    total_loan_orig = sum(loan_orig_sums)
    total_loan_amt = sum(loan_amt_sums)

    # Calculate the percentages
    loan_orig_percents = [round(sum_ / total_loan_orig * 100 , 2) for sum_ in loan_orig_sums]
    loan_amt_percents = [round(sum_ / total_loan_amt * 100 , 2) for sum_ in loan_amt_sums]

    # Create the DataFrame
    data = {
        'Income Category': income_categories + ['Total'],
        'Loans Originated': loan_orig_sums + [total_loan_orig],
        'Percentage of Loans Originated': loan_orig_percents + [100.0],
        'Total Loan Amounts': loan_amt_sums + [total_loan_amt],
        'Percentage of Total Loan Amounts': loan_amt_percents + [100.0]
    }
    census_income_data = pd.DataFrame(data)

    return census_income_data

def calculate_bor_income_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    # Define the categories and corresponding columns
    income_categories = ['Low', 'Moderate', 'Middle', 'Upper', 'Unknown']
    bor_loan_orig_columns = ['Loan_Orig_BILow','Loan_Orig_BIMod', 'Loan_Orig_BIMid','Loan_Orig_BIUpp','Loan_Orig_BIUnk']
    bor_loan_amt_columns = ['Amt_Orig_BILow', 'Amt_Orig_BIMod', 'Amt_Orig_BIMid', 'Amt_Orig_BIUpp', 'Amt_Orig_BIUnk']

    # Calculate the sums for each category
    bor_loan_orig_sums = [df[col].sum() for col in bor_loan_orig_columns]
    bor_loan_amt_sums = [df[col].sum() for col in bor_loan_amt_columns]

    # Calculate the total sums
    bor_total_loan_orig = sum(bor_loan_orig_sums)
    bor_total_loan_amt = sum(bor_loan_amt_sums)

    # Calculate the percentages
    bor_loan_orig_percents = [round(sum_ / bor_total_loan_orig * 100, 2) for sum_ in bor_loan_orig_sums]
    bor_loan_amt_percents = [round(sum_ / bor_total_loan_amt * 100, 2) for sum_ in bor_loan_amt_sums]

    # Create the DataFrame
    bor_data = {
        'Income Category': income_categories + ['Total'],
        'Loans Originated': bor_loan_orig_sums + [bor_total_loan_orig],
        'Percentage of Loans Originated': bor_loan_orig_percents + [100.0],
        'Total Loan Amounts': bor_loan_amt_sums + [bor_total_loan_amt],
        'Percentage of Total Loan Amounts': bor_loan_amt_percents + [100.0]
    }
    bor_income_data = pd.DataFrame(bor_data)

    return bor_income_data

def calculate_combined_income_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    # Define the categories and corresponding columns
    income_categories = ['Low', 'Moderate', 'Middle', 'Upper', 'Unknown']
    bor_loan_orig_columns = ['Loan_Orig_BILow','Loan_Orig_BIMod', 'Loan_Orig_BIMid','Loan_Orig_BIUpp','Loan_Orig_BIUnk']
    bor_loan_amt_columns = ['Amt_Orig_BILow', 'Amt_Orig_BIMod', 'Amt_Orig_BIMid', 'Amt_Orig_BIUpp', 'Amt_Orig_BIUnk']
    loan_orig_columns = ['Loan_Orig_TILow','Loan_Orig_TIMod', 'Loan_Orig_TIMid','Loan_Orig_TIUpp','Loan_Orig_TIUnk']
    loan_amt_columns = ['Amt_Orig_TILow', 'Amt_Orig_TIMod', 'Amt_Orig_TIMid', 'Amt_Orig_TIUpp', 'Amt_Orig_TIUnk']

    # Calculate the sums for each category
    bor_loan_orig_sums = [df[col].sum() for col in bor_loan_orig_columns]
    bor_loan_amt_sums = [df[col].sum() for col in bor_loan_amt_columns]
    loan_orig_sums = [df[col].sum() for col in loan_orig_columns]
    loan_amt_sums = [df[col].sum() for col in loan_amt_columns]

    # Calculate the total sums
    bor_total_loan_orig = sum(bor_loan_orig_sums)
    bor_total_loan_amt = sum(bor_loan_amt_sums)
    total_loan_orig = sum(loan_orig_sums)
    total_loan_amt = sum(loan_amt_sums)

    # Calculate the percentages
    bor_loan_orig_percents = [round(sum_ / bor_total_loan_orig * 100, 2) for sum_ in bor_loan_orig_sums]
    bor_loan_amt_percents = [round(sum_ / bor_total_loan_amt * 100, 2) for sum_ in bor_loan_amt_sums]
    loan_orig_percents = [round(sum_ / total_loan_orig * 100, 2) for sum_ in loan_orig_sums]
    loan_amt_percents = [round(sum_ / total_loan_amt * 100, 2) for sum_ in loan_amt_sums]

    # Create the DataFrames
    bor_data = {
        'Income Category': income_categories + ['Total'],
        'Borrower Loans Originated': bor_loan_orig_sums + [bor_total_loan_orig],
        'Borrower Percentage of Loans Originated': bor_loan_orig_percents + [100.0],
        'Borrower Total Loan Amounts': bor_loan_amt_sums + [bor_total_loan_amt],
        'Borrower Percentage of Total Loan Amounts': bor_loan_amt_percents + [100.0]
    }
    bor_income_data = pd.DataFrame(bor_data)

    census_data = {
        'Income Category': income_categories + ['Total'],
        'Census Loans Originated': loan_orig_sums + [total_loan_orig],
        'Census Percentage of Loans Originated': loan_orig_percents + [100.0],
        'Census Total Loan Amounts': loan_amt_sums + [total_loan_amt],
        'Census Percentage of Total Loan Amounts': loan_amt_percents + [100.0]
    }
    census_income_data = pd.DataFrame(census_data)

    # Combine the DataFrames
    combined_data = pd.concat([bor_income_data.set_index('Income Category'), census_income_data.set_index('Income Category')], axis=1).reset_index()

    return combined_data

def calculate_loan_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    loan_types = ['Home Improvement', 'Home Purchase', 'Multi-Family Purchase', 'Refinance', 'Total']
    loan_orig_columns = ['Loan_Orig_HI', 'Loan_Orig_SFam', 'Loan_Orig_MFam', 'Loan_Orig_HR']
    loan_amt_columns = ['Amt_Orig_HI', 'Amt_Orig_SFam', 'Amt_Orig_MFam', 'Amt_Orig_HR']

    # Calculate the sums for each loan type
    loan_orig_sums = [df[col].sum() for col in loan_orig_columns]
    loan_amt_sums = [df[col].sum() for col in loan_amt_columns]

    # Calculate the total sums
    total_loan_orig = sum(loan_orig_sums)
    total_loan_amt = sum(loan_amt_sums)

    # Add the total sums to the lists
    loan_orig_sums.append(total_loan_orig)
    loan_amt_sums.append(total_loan_amt)

    # Calculate the percentages
    loan_orig_percents = [round(sum_ / total_loan_orig * 100, 2) for sum_ in loan_orig_sums]
    loan_amt_percents = [round(sum_ / total_loan_amt * 100, 2) for sum_ in loan_amt_sums]

    # Create the DataFrame
    data = {
        'Loan Type': loan_types,
        'Loans Originated': loan_orig_sums,
        'Percentage of Loans Originated': loan_orig_percents,
        'Total Loan Amounts': loan_amt_sums,
        'Percentage of Total Loan Amounts': loan_amt_percents
    }
    loan_data = pd.DataFrame(data)

    return loan_data

def plot_loan_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    loan_types = ['Home Improvement', 'Home Purchase', 'Multi-Family Purchase', 'Refinance', 'Total']
    loan_orig_columns = ['Loan_Orig_HI', 'Loan_Orig_SFam', 'Loan_Orig_MFam', 'Loan_Orig_HR']
    loan_amt_columns = ['Amt_Orig_HI', 'Amt_Orig_SFam', 'Amt_Orig_MFam', 'Amt_Orig_HR']

    # Calculate the sums for each loan type
    loan_orig_sums = [df[col].sum() for col in loan_orig_columns]
    loan_amt_sums = [df[col].sum() for col in loan_amt_columns]

    # Calculate the total sums
    total_loan_orig = sum(loan_orig_sums)
    total_loan_amt = sum(loan_amt_sums)

    # Add the total sums to the lists
    loan_orig_sums.append(total_loan_orig)
    loan_amt_sums.append(total_loan_amt)

    # Calculate the percentages
    loan_orig_percents = [sum_ / total_loan_orig * 100 for sum_ in loan_orig_sums]
    loan_amt_percents = [sum_ / total_loan_amt * 100 for sum_ in loan_amt_sums]

    # Create the DataFrame
    data = {
        'Loan Type': loan_types,
        'Loans Originated': loan_orig_sums,
        'Percentage of Loans Originated': loan_orig_percents,
        'Total Loan Amounts': loan_amt_sums,
        'Percentage of Total Loan Amounts': loan_amt_percents
    }
    loan_data = pd.DataFrame(data)

    # Create a bar graph for Loans Originated
    fig = go.Figure(data=[
        go.Bar(name='Loans Originated', x=loan_data['Loan Type'], y=loan_data['Loans Originated']),
        go.Bar(name='Total Loan Amounts', x=loan_data['Loan Type'], y=loan_data['Total Loan Amounts'])
    ])

    # Change the bar mode
    fig.update_layout(barmode='group')

    return fig

def plot_loan_amt_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    income_categories = ['Low', 'Moderate', 'Middle', 'Upper', 'Unknown']
    bor_loan_amt_columns = ['Amt_Orig_BILow', 'Amt_Orig_BIMod', 'Amt_Orig_BIMid', 'Amt_Orig_BIUpp', 'Amt_Orig_BIUnk']

    # Calculate the sums for each category
    bor_loan_amt_sums = [df[col].sum() for col in bor_loan_amt_columns]

    # Create a bar graph for Total Loan Amounts
    fig = go.Figure(data=[
        go.Bar(name='Total Loan Amounts', x=income_categories, y=bor_loan_amt_sums)
    ])

    # Add labels and title
    fig.update_layout(
        title="Total Loan Amounts by Income Level",
        xaxis_title="Income Levels",
        yaxis_title="Amount lent in 000s",
    )

    return fig

def plot_loan_orig_data(df, id_rssd):
    # Filter the DataFrame based on the id_rssd
    df = df[df['id_rssd'] == id_rssd]

    income_categories = ['Low', 'Moderate', 'Middle', 'Upper', 'Unknown']
    bor_loan_orig_columns = ['Loan_Orig_BILow','Loan_Orig_BIMod', 'Loan_Orig_BIMid','Loan_Orig_BIUpp','Loan_Orig_BIUnk']

    # Calculate the sums for each category
    bor_loan_orig_sums = [df[col].sum() for col in bor_loan_orig_columns]

    # Create a bar graph for Loans Originated
    fig = go.Figure(data=[
        go.Bar(name='Loans Originated', x=income_categories, y=bor_loan_orig_sums)
    ])

    fig.update_layout(
        title="Total Loan Originations by Income Level",
        xaxis_title="Income Levels",
        yaxis_title="Loans Originated",
    )

    return fig