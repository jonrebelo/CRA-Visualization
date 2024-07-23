
import plotly.graph_objects as go
import polars as pl
from great_tables import GT, style, loc
import pandas as pd

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

def create_inside_out_great_table(df, selected_bank, selected_area):

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

    group_labels = ['Residential'] * 4 + ['Business'] * 3 + ['Total'] * 1
    new_df.insert(0, 'Group', group_labels)

    new_df.columns = [
        'Group','a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'
    ]

    # Create a Great Tables instance with the new dataframe
    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style=2, color="blue")
    .tab_stub(rowname_col="a", groupname_col="Group")
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
    .tab_header(title = "Assessment Area Distribution", subtitle=f"{selected_bank} in {selected_area}")
    .tab_spanner(label="Inside", columns=['b', 'h', 'c', 'i'])
    .tab_spanner(label="Outside", columns=['d', 'j', 'e', 'k'])
    .tab_spanner(label="Totals", columns=['f', 'l', 'g', 'm'])
    .fmt_percent(columns=['h', 'i', 'j', 'k', 'l', 'm'], decimals=1)
    .fmt_number(columns=[ 'b', 'c', 'd', 'e', 'f', 'g' ], decimals=0, use_seps=True)
    .cols_align(align="center", columns=[ 'Group','a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'])
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
        container_width="100%",
        stub_font_weight="bold",       # Make stub text bold
        stub_font_size="14px",         # Adjust stub font size if needed
        stub_background_color="lightgray",  # Set stub background color if desired
        stub_border_style="solid",     # Set stub border style
        stub_border_color="gray",      # Set stub border color
        row_group_font_weight="bold",      # Make row group labels bold
        row_group_font_size="16px",        # Adjust row group font size if needed
        row_group_background_color="lightblue",  # Set row group background color if desired
        row_group_padding="8px",           # Add padding around row group labels
    )
    .opt_vertical_padding(scale=1.5)
    .opt_horizontal_padding(scale=1.2)
    
)
    return gt_instance

def bor_income_table(df, selected_bank, selected_area):
    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    # Define the new structure
    income_levels = ['Low', 'Moderate', 'SubTotal', 'Low', 'Moderate', 'SubTotal']
    counts = df[['Loan_Orig_SFam_Closed_BILow', 'Loan_Orig_SFam_Closed_BIMod', 'Loan_Orig_SFam_Closed', 'Loan_Orig_SFam_Open_BILow', 'Loan_Orig_SFam_Open_BIMod', 'Loan_Orig_SFam_Open']].values[0]
    agg_counts = df[['Agg_Loan_Orig_SFam_Closed_BILow', 'Agg_Loan_Orig_SFam_Closed_BIMod', 'Agg_Loan_Orig_SFam_Closed', 'Agg_Loan_Orig_SFam_Open_BILow', 'Agg_Loan_Orig_SFam_Open_BIMod', 'Agg_Loan_Orig_SFam_Open']].values[0]
    dollars = df[['Amt_Orig_SFam_Closed_BILow', 'Amt_Orig_SFam_Closed_BIMod', 'Amt_Orig_SFam_Closed', 'Amt_Orig_SFam_Open_BILow', 'Amt_Orig_SFam_Open_BIMod', 'Amt_Orig_SFam_Open']].values[0]
    agg_dollars = df[['Agg_Amt_Orig_SFam_Closed_BILow', 'Agg_Amt_Orig_SFam_Closed_BIMod', 'Agg_Amt_Orig_SFam_Closed', 'Agg_Amt_Orig_SFam_Open_BILow', 'Agg_Amt_Orig_SFam_Open_BIMod', 'Agg_Amt_Orig_SFam_Open']].values[0]

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Income Level': income_levels,
        'Count': counts,
        'Agg Count': agg_counts,
        'Dollar': dollars,
        'Agg Dollar': agg_dollars
    })
    # Update rows 8 and 9 with the sums
    low_values = new_df.loc[0, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] + new_df.loc[3, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
    moderate_values = new_df.loc[1, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] + new_df.loc[4, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
    
    # Create the rows for 'Low' and 'Moderate'
    low_row = pd.DataFrame({
        'Income Level': ['Low'],
        'Count': [low_values['Count']],
        'Agg Count': [low_values['Agg Count']],
        'Dollar': [low_values['Dollar']],
        'Agg Dollar': [low_values['Agg Dollar']]
    })
    moderate_row = pd.DataFrame({
        'Income Level': ['Moderate'],
        'Count': [moderate_values['Count']],
        'Agg Count': [moderate_values['Agg Count']],
        'Dollar': [moderate_values['Dollar']],
        'Agg Dollar': [moderate_values['Agg Dollar']]
    })
    
    # Insert the rows into the DataFrame
    new_df = pd.concat([new_df, low_row, moderate_row]).reset_index(drop=True)

    print(new_df)

    #Iterate over the DataFrame in steps of 3 (Low, Moderate, Total)
    for i in range(2, len(new_df), 4):
        # Calculate the 'Other' values
        other_values = new_df.loc[i, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] - new_df.loc[i-1, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] - new_df.loc[i-2, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
        # Create the 'Other' row
        other_row = pd.DataFrame({
            'Income Level': ['Other'],
            'Count': [other_values['Count']],
            'Agg Count': [other_values['Agg Count']],
            'Dollar': [other_values['Dollar']],
            'Agg Dollar': [other_values['Agg Dollar']]
        })
        # Insert the 'Other' row into the DataFrame
        new_df = pd.concat([new_df.iloc[:i], other_row, new_df.iloc[i:]]).reset_index(drop=True)
    
    total_values = new_df.loc[3, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] + new_df.loc[7, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
    # Create the 'Total' row
    total_row = pd.DataFrame({
        'Income Level': ['Total'],
        'Count': [total_values['Count']],
        'Agg Count': [total_values['Agg Count']],
        'Dollar': [total_values['Dollar']],
        'Agg Dollar': [total_values['Agg Dollar']]
    })
    # Append the 'Total' row to the DataFrame
    new_df = new_df._append(total_row, ignore_index=True)

    other_values = new_df.loc[10, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] - new_df.loc[9, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] - new_df.loc[8, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
    # Create the 'Other' row
    other_row = pd.DataFrame({
        'Income Level': ['Other'],
        'Count': [other_values['Count']],
        'Agg Count': [other_values['Agg Count']],
        'Dollar': [other_values['Dollar']],
        'Agg Dollar': [other_values['Agg Dollar']]
    })
    # Insert the 'Other' row into the DataFrame before the last 'Total' row
    new_df = pd.concat([new_df.iloc[:-1], other_row, new_df.iloc[-1:]]).reset_index(drop=True)

    # Add new columns for percentages
    new_df['Count %'] = 0
    new_df['Agg Count %'] = 0
    new_df['Dollar %'] = 0
    new_df['Agg Dollar %'] = 0

# Calculate percentages
    for i in range(len(new_df)):
        if i < 4:
            divisor = new_df.loc[3, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
        elif i < 8:
            divisor = new_df.loc[7, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
        else:
            divisor = new_df.loc[11, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]

        new_df.loc[i, 'Count %'] = new_df.loc[i, 'Count'] / divisor['Count']
        new_df.loc[i, 'Agg Count %'] = new_df.loc[i, 'Agg Count'] / divisor['Agg Count']
        new_df.loc[i, 'Dollar %'] = new_df.loc[i, 'Dollar'] / divisor['Dollar']
        new_df.loc[i, 'Agg Dollar %'] = new_df.loc[i, 'Agg Dollar'] / divisor['Agg Dollar']

    

    column_order = ['Income Level', 'Count', 'Count %', 'Agg Count', 'Agg Count %', 'Dollar', 'Dollar %', 'Agg Dollar', 'Agg Dollar %']
    new_df = new_df.reindex(columns=column_order)

    new_df = new_df.fillna(0)

    group_labels = ['Single Family Closed-End'] * 4 + ['Revolving'] * 4 + ['Totals'] * 4
    new_df.insert(0, 'Group', group_labels)

    new_df.columns = [
        'Group', 'Income Level', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style = 2, color = "blue")
    .tab_header(title = "Borrower Low-Moderate Income Lending", subtitle = f"{selected_bank} in {selected_area}")
    .cols_label(a = '#', b = "%", c = "#", d="%", e = '000s', f = '%', g = '000s', h = '%')
    .fmt_percent(columns = ['b', 'd', 'f' , 'h'], decimals = 1)
    .fmt_number(columns= ['a', 'c', 'e', 'g' ], use_seps = True, decimals = 0)
    .tab_style(
        style=style.text(weight="bold"),
        locations=loc.body(columns="Income Level",rows=[3,7,11])
    )
    .tab_stub(rowname_col="Income Level", groupname_col="Group")
    .tab_style(
        style=style.text(style = "italic",),
        locations=loc.body(rows=[3, 7]),
    )
    .tab_style(
        style=style.fill(color="lightyellow"),
        locations=loc.body(rows=[3, 7]),
    )
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[11]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[11]),
    )
    .tab_spanner(label="Bank Count", columns=['a', 'b'])
    .tab_spanner(label="Agg Count", columns=['c', 'd'])
    .tab_spanner(label="Bank $", columns=['e', 'f'])
    .tab_spanner(label="Agg $", columns=['g', 'h'])
    .cols_align(align="center", columns=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'])
    .tab_options(
    stub_font_weight="bold",       # Make stub text bold
    stub_font_size="14px",         # Adjust stub font size if needed
    stub_background_color="lightgray",  # Set stub background color if desired
    stub_border_style="solid",     # Set stub border style
    stub_border_color="gray",      # Set stub border color
    row_group_font_weight="bold",      # Make row group labels bold
    row_group_font_size="16px",        # Adjust row group font size if needed
    row_group_background_color="lightblue",  # Set row group background color if desired
    row_group_padding="8px",           # Add padding around row group labels
    table_body_hlines_style="solid",
    table_body_vlines_style="solid",
    table_body_border_top_color="gray",
    table_body_border_bottom_color="gray",
    container_width = "100%"   
    )
    )

    return gt_instance


def tract_income_table(df, selected_bank, selected_area):
    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    # Define the new structure
    income_levels = ['Low', 'Moderate', 'SubTotal', 'Low', 'Moderate', 'SubTotal', 'Low', 'Moderate', 'SubTotal', 'Low', 'Moderate', 'Total']
    counts = df[['Loan_Orig_SFam_Closed_TILow', 'Loan_Orig_SFam_Closed_TIMod', 'Loan_Orig_SFam_Closed', 'Loan_Orig_SFam_Open_TILow', 'Loan_Orig_SFam_Open_TIMod', 'Loan_Orig_SFam_Open', 'Loan_Orig_MFam_TILow', 'Loan_Orig_MFam_TIMod', 'Loan_Orig_MFam', 'Loan_Orig_TILow', 'Loan_Orig_TIMod','Loan_Orig' ]].values[0]
    agg_counts = df[['Agg_Loan_Orig_SFam_Closed_TILow', 'Agg_Loan_Orig_SFam_Closed_TIMod', 'Agg_Loan_Orig_SFam_Closed', 'Agg_Loan_Orig_SFam_Open_TILow', 'Agg_Loan_Orig_SFam_Open_TIMod', 'Agg_Loan_Orig_SFam_Open', 'Agg_Loan_Orig_MFam_TILow', 'Agg_Loan_Orig_MFam_TIMod', 'Agg_Loan_Orig_MFam', 'Agg_Loan_Orig_TILow', 'Agg_Loan_Orig_TIMod', 'Agg_Loan_Orig']].values[0]
    dollars = df[['Amt_Orig_SFam_Closed_TILow', 'Amt_Orig_SFam_Closed_TIMod', 'Amt_Orig_SFam_Closed', 'Amt_Orig_SFam_Open_TILow', 'Amt_Orig_SFam_Open_TIMod', 'Amt_Orig_SFam_Open','Amt_Orig_MFam_TILow', 'Amt_Orig_MFam_TIMod','Amt_Orig_MFam','Amt_Orig_TILow', 'Amt_Orig_TIMod','Amt_Orig']].values[0]
    agg_dollars = df[['Agg_Amt_Orig_SFam_Closed_TILow', 'Agg_Amt_Orig_SFam_Closed_TIMod', 'Agg_Amt_Orig_SFam_Closed', 'Agg_Amt_Orig_SFam_Open_TILow', 'Agg_Amt_Orig_SFam_Open_TIMod', 'Agg_Amt_Orig_SFam_Open', 'Agg_Amt_Orig_MFam_TILow', 'Agg_Amt_Orig_MFam_TIMod', 'Agg_Amt_Orig_MFam', 'Agg_Amt_Orig_TILow', 'Agg_Amt_Orig_TIMod', 'Agg_Amt_Orig']].values[0]

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Income Level': income_levels,
        'Count': counts,
        'Agg Count': agg_counts,
        'Dollar': dollars,
        'Agg Dollar': agg_dollars
    })

    print(new_df)

    #Iterate over the DataFrame in steps of 4 (Low, Moderate, Total)
    for i in range(2, len(new_df), 4):
        # Calculate the 'Other' values
        other_values = new_df.loc[i, ['Count', 'Agg Count']] - new_df.loc[i-1, ['Count', 'Agg Count']] - new_df.loc[i-2, ['Count', 'Agg Count']]
        # Create the 'Other' row
        other_row = pd.DataFrame({
            'Income Level': ['Other'],
            'Count': [other_values['Count']],
            'Agg Count': [other_values['Agg Count']]
        })
        # Insert the 'Other' row into the DataFrame
        new_df = pd.concat([new_df.iloc[:i], other_row, new_df.iloc[i:]]).reset_index(drop=True)

    other_values = new_df.loc[14, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] - new_df.loc[13, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']] - new_df.loc[12, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
    # Create the 'Other' row
    other_row = pd.DataFrame({
        'Income Level': ['Other'],
        'Count': [other_values['Count']],
        'Agg Count': [other_values['Agg Count']],
        'Dollar': [other_values['Dollar']],
        'Agg Dollar': [other_values['Agg Dollar']]
    })
    # Insert the 'Other' row into the DataFrame before the last 'Total' row
    new_df = pd.concat([new_df.iloc[:-1], other_row, new_df.iloc[-1:]]).reset_index(drop=True)

    # Add new columns for percentages
    new_df['Count %'] = 0
    new_df['Agg Count %'] = 0
    new_df['Dollar %'] = 0
    new_df['Agg Dollar %'] = 0

# Calculate percentages
    for i in range(len(new_df)):
        if i < 4:
            divisor = new_df.loc[3, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
        elif i < 8:
            divisor = new_df.loc[7, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
        elif i < 12:
            divisor = new_df.loc[11, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]
        else:
            divisor = new_df.loc[15, ['Count', 'Agg Count', 'Dollar', 'Agg Dollar']]

        new_df.loc[i, 'Count %'] = new_df.loc[i, 'Count'] / divisor['Count']
        new_df.loc[i, 'Agg Count %'] = new_df.loc[i, 'Agg Count'] / divisor['Agg Count']
        new_df.loc[i, 'Dollar %'] = new_df.loc[i, 'Dollar'] / divisor['Dollar']
        new_df.loc[i, 'Agg Dollar %'] = new_df.loc[i, 'Agg Dollar'] / divisor['Agg Dollar']

    column_order = ['Income Level', 'Count', 'Count %', 'Agg Count', 'Agg Count %', 'Dollar', 'Dollar %', 'Agg Dollar', 'Agg Dollar %']
    new_df = new_df.reindex(columns=column_order)

    new_df = new_df.fillna(0)

    group_labels = ['Single Family Closed-End'] * 4 + ['Revolving'] * 4 + ['Multi-Family'] * 4 + ['Totals'] * 4
    new_df.insert(0, 'Group', group_labels)

    new_df.columns = [
        'Group', 'Income Level', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
    
    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style = 2, color = "blue")
    .tab_header(title = "Low-Moderate Income Tract Lending", subtitle = f"{selected_bank} in {selected_area}")
    .cols_label(a = '#', b = "%", c = "#", d="%", e = '000s', f = '%', g = '000s', h = '%')
    .fmt_percent(columns = ['b', 'd', 'f' , 'h'], decimals = 1)
    .fmt_number(columns= ['a', 'c', 'e', 'g' ], use_seps = True, decimals = 0)
    .tab_style(
        style=style.text(weight="bold"),
        locations=loc.body(columns="Income Level",rows=[3,7,11,15])
    )
    .tab_stub(rowname_col="Income Level", groupname_col="Group")
    .tab_style(
        style=style.text(style = "italic",),
        locations=loc.body(rows=[3, 7, 11]),
    )
    .tab_style(
        style=style.fill(color="lightyellow"),
        locations=loc.body(rows=[3, 7, 11]),
    )
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[15]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[15]),
    )
    .tab_spanner(label="Bank Count", columns=['a', 'b'])
    .tab_spanner(label="Agg Count", columns=['c', 'd'])
    .tab_spanner(label="Bank $", columns=['e', 'f'])
    .tab_spanner(label="Agg $", columns=['g', 'h'])
    .cols_align(align="center", columns=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'])
    .tab_options(
    stub_font_weight="bold",       # Make stub text bold
    stub_font_size="14px",         # Adjust stub font size if needed
    stub_background_color="lightgray",  # Set stub background color if desired
    stub_border_style="solid",     # Set stub border style
    stub_border_color="gray",      # Set stub border color
    row_group_font_weight="bold",      # Make row group labels bold
    row_group_font_size="16px",        # Adjust row group font size if needed
    row_group_background_color="lightblue",  # Set row group background color if desired
    row_group_padding="8px",           # Add padding around row group labels
    table_body_hlines_style="solid",
    table_body_vlines_style="solid",
    table_body_border_top_color="gray",
    table_body_border_bottom_color="gray",
    container_width = "100%"   
    )
    )

    return gt_instance

def business_tract_table(df, selected_bank, selected_area):
    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    # Define the new structure
    income_levels = ['Low', 'Moderate', 'SubTotal', 'Low', 'Moderate', 'SubTotal']
    counts = df[['SB_Loan_Orig_TILow', 'SB_Loan_Orig_TIMod', 'SB_Loan_Orig', 'SF_Loan_Orig_TILow', 'SF_Loan_Orig_TIMod', 'SF_Loan_Orig']].values[0]
    agg_counts = df[['Agg_SB_Loan_Purch_TILow', 'Agg_SB_Loan_Orig_TIMod', 'Agg_SB_Loan_Orig', 'Agg_SF_Loan_Orig_TILow', 'Agg_SF_Loan_Orig_TIMod', 'Agg_SF_Loan_Orig']].values[0]
    

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Income Level': income_levels,
        'Count': counts,
        'Agg Count': agg_counts,
    })

    low_values = new_df.loc[0, ['Count', 'Agg Count']] + new_df.loc[3, ['Count', 'Agg Count']]
    moderate_values = new_df.loc[1, ['Count', 'Agg Count']] + new_df.loc[4, ['Count', 'Agg Count']]
    
    # Create the rows for 'Low' and 'Moderate'
    low_row = pd.DataFrame({
        'Income Level': ['Low'],
        'Count': [low_values['Count']],
        'Agg Count': [low_values['Agg Count']]
    })
    moderate_row = pd.DataFrame({
        'Income Level': ['Moderate'],
        'Count': [moderate_values['Count']],
        'Agg Count': [moderate_values['Agg Count']]
    })

    # Insert the rows into the DataFrame
    new_df = pd.concat([new_df, low_row, moderate_row]).reset_index(drop=True)

    for i in range(2, len(new_df), 4):
    # Calculate the 'Other' values
        other_values = new_df.loc[i, ['Count', 'Agg Count']] - new_df.loc[i-1, ['Count', 'Agg Count']] - new_df.loc[i-2, ['Count', 'Agg Count']]
        # Create the 'Other' row
        other_row = pd.DataFrame({
            'Income Level': ['Other'],
            'Count': [other_values['Count']],
            'Agg Count': [other_values['Agg Count']]
        })
        # Insert the 'Other' row into the DataFrame
        new_df = pd.concat([new_df.iloc[:i], other_row, new_df.iloc[i:]]).reset_index(drop=True)

    total_values = new_df.loc[3, ['Count', 'Agg Count']] + new_df.loc[7, ['Count', 'Agg Count']]
    # Create the 'Total' row
    total_row = pd.DataFrame({
        'Income Level': ['Total'],
        'Count': [total_values['Count']],
        'Agg Count': [total_values['Agg Count']]
    })
    # Append the 'Total' row to the DataFrame
    new_df = new_df._append(total_row, ignore_index=True)

    other_values = new_df.loc[10, ['Count', 'Agg Count']] - new_df.loc[9, ['Count', 'Agg Count']] - new_df.loc[8, ['Count', 'Agg Count']]
    # Create the 'Other' row
    other_row = pd.DataFrame({
        'Income Level': ['Other'],
        'Count': [other_values['Count']],
        'Agg Count': [other_values['Agg Count']],
    })
    # Insert the 'Other' row into the DataFrame before the last 'Total' row
    new_df = pd.concat([new_df.iloc[:-1], other_row, new_df.iloc[-1:]]).reset_index(drop=True)

    # Add new columns for percentages
    new_df['Count %'] = 0
    new_df['Agg Count %'] = 0

# Calculate percentages
    for i in range(len(new_df)):
        if i < 4:
            divisor = new_df.loc[3, ['Count', 'Agg Count']]
        elif i < 8:
            divisor = new_df.loc[7, ['Count', 'Agg Count']]
        else:
            divisor = new_df.loc[11, ['Count', 'Agg Count']]

        new_df.loc[i, 'Count %'] = new_df.loc[i, 'Count'] / divisor['Count']
        new_df.loc[i, 'Agg Count %'] = new_df.loc[i, 'Agg Count'] / divisor['Agg Count']

    column_order = ['Income Level', 'Count', 'Count %', 'Agg Count', 'Agg Count %']
    new_df = new_df.reindex(columns=column_order)

    new_df = new_df.fillna(0)

    group_labels = ['Small Business'] * 4 + ['Farm'] * 4 + ['Totals'] * 4
    new_df.insert(0, 'Group', group_labels)

    new_df.columns = [
        'Group', 'Income Level', 'a', 'b', 'c', 'd']

    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style = 2, color = "blue")
    .tab_header(title = "Business Tract Lending", subtitle = f"{selected_bank} in {selected_area}")
    .cols_label(a = '#', b = "%", c = "#", d="%")
    .fmt_percent(columns = ['b', 'd'], decimals = 1)
    .fmt_number(columns= ['a', 'c'], use_seps = True, decimals = 0)
    .tab_style(
        style=style.text(weight="bold"),
        locations=loc.body(columns="Income Level",rows=[3,7,11])
    )
    .tab_stub(rowname_col="Income Level", groupname_col="Group")
    .tab_style(
        style=style.text(style = "italic",),
        locations=loc.body(rows=[3, 7]),
    )
    .tab_style(
        style=style.fill(color="lightyellow"),
        locations=loc.body(rows=[3, 7]),
    )
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[11]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[11]),
    )
    .tab_spanner(label="Bank Count", columns=['a', 'b'])
    .tab_spanner(label="Agg Count", columns=['c', 'd'])
    .cols_align(align="center", columns=['a', 'b', 'c', 'd'])
    .tab_options(
    stub_font_weight="bold",       # Make stub text bold
    stub_font_size="14px",         # Adjust stub font size if needed
    stub_background_color="lightgray",  # Set stub background color if desired
    stub_border_style="solid",     # Set stub border style
    stub_border_color="gray",      # Set stub border color
    row_group_font_weight="bold",      # Make row group labels bold
    row_group_font_size="16px",        # Adjust row group font size if needed
    row_group_background_color="lightblue",  # Set row group background color if desired
    row_group_padding="8px",           # Add padding around row group labels
    table_body_hlines_style="solid",
    table_body_vlines_style="solid",
    table_body_border_top_color="gray",
    table_body_border_bottom_color="gray",
    container_width = "100%"   
    )
    )

    return gt_instance

def business_size_table(df, selected_bank, selected_area):

    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    # Define the new structure
    business_size = ['Less than $1M', 'SubTotal', 'Less than $1M', 'SubTotal']
    counts = df[['SB_Loan_Orig_GAR_less_1m', 'SB_Loan_Orig', 'SF_Loan_Orig_GAR_less_1m', 'SF_Loan_Orig']].values[0]
    agg_counts = df[['Agg_SB_Loan_Orig_GAR_less_1m', 'Agg_SB_Loan_Orig', 'Agg_SF_Loan_Orig_GAR_less_1m', 'Agg_SF_Loan_Orig']].values[0]

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Business Size': business_size,
        'Count': counts,
        'Agg Count': agg_counts,
    })

    #Iterate over the DataFrame in steps of 3 (Low, Moderate, Total)
    for i in range(1, len(new_df), 3):
    # Calculate the 'Other' values
        other_values = new_df.loc[i, ['Count', 'Agg Count']] - new_df.loc[i-1, ['Count', 'Agg Count']]
        # Create the 'Other' row
        other_row = pd.DataFrame({
            'Business Size': ['Greater than $1M'],
            'Count': [other_values['Count']],
            'Agg Count': [other_values['Agg Count']]
        })
        # Insert the 'Other' row into the DataFrame
        new_df = pd.concat([new_df.iloc[:i], other_row, new_df.iloc[i:]]).reset_index(drop=True)

    other_values = new_df.loc[4, ['Count', 'Agg Count']] - new_df.loc[3, ['Count', 'Agg Count']]
    # Create the 'Other' row
    other_row = pd.DataFrame({
        'Business Size': ['Greater than $1M'],
        'Count': [other_values['Count']],
        'Agg Count': [other_values['Agg Count']],
    })
    # Insert the 'Other' row into the DataFrame before the last 'Total' row
    new_df = pd.concat([new_df.iloc[:-1], other_row, new_df.iloc[-1:]]).reset_index(drop=True)

    small_values = new_df.loc[0, ['Count', 'Agg Count']] + new_df.loc[3, ['Count', 'Agg Count']]
    large_values = new_df.loc[1, ['Count', 'Agg Count']] + new_df.loc[4, ['Count', 'Agg Count']]
    
    # Create the rows for 'Low' and 'Moderate'
    small_row = pd.DataFrame({
        'Business Size': ['Less than $1M'],
        'Count': [small_values['Count']],
        'Agg Count': [small_values['Agg Count']]
    })
    large_row = pd.DataFrame({
        'Business Size': ['Greater than $1M'],
        'Count': [large_values['Count']],
        'Agg Count': [large_values['Agg Count']]
    })

    # Insert the rows into the DataFrame
    new_df = pd.concat([new_df, small_row, large_row]).reset_index(drop=True)

    total_values = new_df.loc[2, ['Count', 'Agg Count']] + new_df.loc[5, ['Count', 'Agg Count']]
    # Create the 'Total' row
    total_row = pd.DataFrame({
        'Business Size': ['Total'],
        'Count': [total_values['Count']],
        'Agg Count': [total_values['Agg Count']]
    })
    # Append the 'Total' row to the DataFrame
    new_df = new_df._append(total_row, ignore_index=True)

    print(new_df)

    # Add new columns for percentages
    new_df['Count %'] = 0
    new_df['Agg Count %'] = 0

# Calculate percentages
    for i in range(len(new_df)):
        if i < 3:
            divisor = new_df.loc[2, ['Count', 'Agg Count']]
        elif i < 6:
            divisor = new_df.loc[5, ['Count', 'Agg Count']]
        else:
            divisor = new_df.loc[8, ['Count', 'Agg Count']]

        new_df.loc[i, 'Count %'] = new_df.loc[i, 'Count'] / divisor['Count']
        new_df.loc[i, 'Agg Count %'] = new_df.loc[i, 'Agg Count'] / divisor['Agg Count']

    column_order = ['Business Size', 'Count', 'Count %', 'Agg Count', 'Agg Count %']
    new_df = new_df.reindex(columns=column_order)

    new_df = new_df.fillna(0)

    group_labels = ['Business Size (By Revenue)'] * 3 + ['Farm Size (By Revenue)'] * 3 + ['Totals'] * 3
    new_df.insert(0, 'Group', group_labels)

    new_df.columns = [
        'Group', 'Income Level', 'a', 'b', 'c', 'd']

    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style = 2, color = "blue")
    .tab_header(title = "Business Size Lending", subtitle = f"{selected_bank} in {selected_area}")
    .cols_label(a = '#', b = "%", c = "#", d="%")
    .fmt_percent(columns = ['b', 'd'], decimals = 1)
    .fmt_number(columns= ['a', 'c'], use_seps = True, decimals = 0)
    .tab_style(
        style=style.text(weight="bold"),
        locations=loc.body(columns="Income Level",rows=[2,5,8])
    )
    .tab_stub(rowname_col="Income Level", groupname_col="Group")
    .tab_style(
        style=style.text(style = "italic",),
        locations=loc.body(rows=[2, 5]),
    )
    .tab_style(
        style=style.fill(color="lightyellow"),
        locations=loc.body(rows=[2, 5]),
    )
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[8]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[8]),
    )
    .tab_spanner(label="Bank Loan Count", columns=['a', 'b'])
    .tab_spanner(label="Agg Loan Count", columns=['c', 'd'])
    .cols_align(align="center", columns=['a', 'b', 'c', 'd'])
    .tab_options(
    stub_font_weight="bold",       # Make stub text bold
    stub_font_size="14px",         # Adjust stub font size if needed
    stub_background_color="lightgray",  # Set stub background color if desired
    stub_border_style="solid",     # Set stub border style
    stub_border_color="gray",      # Set stub border color
    row_group_font_weight="bold",      # Make row group labels bold
    row_group_font_size="16px",        # Adjust row group font size if needed
    row_group_background_color="lightblue",  # Set row group background color if desired
    row_group_padding="8px",           # Add padding around row group labels
    table_body_hlines_style="solid",
    table_body_vlines_style="solid",
    table_body_border_top_color="gray",
    table_body_border_bottom_color="gray",
    container_width = "100%"   
    )
    )

    return gt_instance

def demographics_table(df, selected_bank, selected_area):
    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    demo = ['OO_Low', '00_Mod', 'OO_Total', 'Units_Low', 'Units_Mod', 'Units_Total', 'Income_Low', 'Income_Mod','Family_Count']
    inside_counts = df[['Owner_Occupied_Units_TILow_Inside', 'Owner_Occupied_Units_TIMod_Inside', 'Owner_Occupied_Units_Inside', 'Total5orMoreHousingUnitsInStructure_TILow_Inside', 'Total5orMoreHousingUnitsInStructure_TIMod_Inside','Total5orMoreHousingUnitsInStructure_Inside','Low_Income_Family_Count_Inside','Moderate_Income_Family_Count_Inside','Family_Count_Inside'
    ]].values[0]
    total_counts = df[['Owner_Occupied_Units_TILow','Owner_Occupied_Units_TIMod','Owner_Occupied_Units','Total5orMoreHousingUnitsInStructure_TILow','Total5orMoreHousingUnitsInStructure_TIMod','Total5orMoreHousingUnitsInStructure','Low_Income_Family_Count','Moderate_Income_Family_Count','Family_Count',
    ]].values[0]

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Demographic': demo,
        'Count': inside_counts,
        'Agg_Count': total_counts,
    })

    # Calculate the difference between 'Agg Count' and 'Count'
    count_diff = new_df['Agg_Count'] - new_df['Count']

    # Insert the 'Count_Dif' column at position 2
    new_df.insert(2, 'Count_Dif', count_diff)

    # Calculate the percentages
    count_per = new_df['Count'] / new_df['Agg_Count']
    count_dif_per = new_df['Count_Dif'] / new_df['Agg_Count']
    agg_count_per = new_df['Agg_Count'] / new_df['Agg_Count']

    # Insert the percentage columns
    new_df.insert(2, 'Count_Per', count_per)
    new_df.insert(4, 'Count_Dif_Per', count_dif_per)
    new_df.insert(6, 'Agg_Count_Per', agg_count_per)

    group_labels = ['Owner Occupied Units'] * 3 + ['Housing With 5+ Units'] * 3 + ['Family Counts'] * 3
    new_df.insert(0, 'Group', group_labels)

    new_df['Demographic'] = new_df['Demographic'].replace({'OO_Low': 'Low Income', '00_Mod': 'Moderate Income', 'OO_Total': 'Total', 'Units_Low': 'Low Income', 'Units_Mod': 'Moderate Income', 'Units_Total': 'Total', 'Income_Low': 'Low Income', 'Income_Mod': 'Moderate Income', 'Family_Count': 'Total'})

    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style=2, color="blue")
    .tab_stub(rowname_col="Demographic", groupname_col="Group")
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[2,5,8]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[2,5,8]),
    )
    .tab_header(title = "Assessment Area Residential Demographics", subtitle=f"{selected_bank} in {selected_area}")
    .tab_spanner(label="Inside", columns=['Count', 'Count_Per'])
    .tab_spanner(label="Outside", columns=['Count_Dif', 'Count_Dif_Per'])
    .tab_spanner(label="Totals", columns=['Agg_Count', 'Agg_Count_Per'])
    .fmt_percent(columns=['Count_Per', 'Count_Dif_Per', 'Agg_Count_Per'], decimals=1)
    .fmt_number(columns=[ 'Count', 'Count_Dif', 'Agg_Count' ], decimals=0, use_seps=True)
    .cols_align(align="center", columns=[ 'Group','Demographic', 'Count', 'Count_Per', 'Count_Dif', 'Count_Dif_Per', 'Agg_Count', 'Agg_Count_Per'])
    .cols_label(Count = '#', Count_Per = '%', Count_Dif = '#',Count_Dif_Per = '%', Agg_Count = '#', Agg_Count_Per= '%')
    .tab_options(
        table_body_hlines_style="solid",
        table_body_vlines_style="solid",
        table_body_border_top_color="gray",
        table_body_border_bottom_color="gray",
        container_width="100%",
        stub_font_weight="bold",       # Make stub text bold
        stub_font_size="14px",         # Adjust stub font size if needed
        stub_background_color="lightgray",  # Set stub background color if desired
        stub_border_style="solid",     # Set stub border style
        stub_border_color="gray",      # Set stub border color
        row_group_font_weight="bold",      # Make row group labels bold
        row_group_font_size="16px",        # Adjust row group font size if needed
        row_group_background_color="lightblue",  # Set row group background color if desired
        row_group_padding="8px",           # Add padding around row group labels
    )
    .opt_vertical_padding(scale=1.5)
    .opt_horizontal_padding(scale=1.2)
    
)
    return gt_instance

def business_demographics_table(df, selected_bank, selected_area):
    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    demo = ['SB_Low', 'SB_Mod', 'SB_Total', 'All_Small', 'Low_Rev', 'Middle_Rev', 'SF_Low', 'SF_Mod', 'SF_Total', 'SF_All_Small', 'SF_Low_Rev', 'SF_Middle_Rev']

    inside_counts = df[[
    'Establishments_Small_Business_TILow_Inside',
    'Establishments_Small_Business_TIMod_Inside',
    'Establishments_Small_Business_Inside',
    'Establishments_GAR_Less_1M_Small_Business_Inside','Establishments_GAR_1_to_250k_Small_Business_Inside','Establishments_GAR_250k_to_1M_Small_Business_Inside',
    'Establishments_Small_Farm_TILow_Inside',
    'Establishments_Small_Farm_TIMod_Inside',
    'Establishments_Small_Farm_Inside',
    'Establishments_GAR_Less_1M_Small_Farm_Inside',
    'Establishments_GAR_1_to_250k_Small_Farm_Inside',
    'Establishments_GAR_250k_to_1M_Small_Farm_Inside'
    ]].values[0]

    total_counts = df[[
    'Establishments_Small_Business_TILow',
    'Establishments_Small_Business_TIMod',
    'Establishments_Small_Business',
    'Establishments_GAR_Less_1M_Small_Business',
    'Establishments_GAR_1_to_250k_Small_Business',
    'Establishments_GAR_250k_to_1M_Small_Business',
    'Establishments_Small_Farm_TILow',
    'Establishments_Small_Farm_TIMod',
    'Establishments_Small_Farm',
    'Establishments_GAR_Less_1M_Small_Farm',
    'Establishments_GAR_1_to_250k_Small_Farm',
    'Establishments_GAR_250k_to_1M_Small_Farm'
    ]].values[0]

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Demographic': demo,
        'Count': inside_counts,
        'Agg_Count': total_counts,
    })

    print(new_df)

    new_rows_1 = pd.DataFrame({
    'Demographic': ['SB_1M'],
    'Count': [
        (df['Establishments_Small_Business_Inside'] - df['Establishments_GAR_Less_1M_Small_Business_Inside']).values[0]
    ],
    'Agg_Count': [
        (df['Establishments_Small_Business'] - df['Establishments_GAR_Less_1M_Small_Business']).values[0]
    ]
})

    new_rows_2 = pd.DataFrame({
    'Demographic': ['SF_1M'],
    'Count': [
        (df['Establishments_Small_Farm_Inside'] - df['Establishments_GAR_Less_1M_Small_Farm_Inside']).values[0]
    ],
    'Agg_Count': [
        (df['Establishments_Small_Farm'] - df['Establishments_GAR_Less_1M_Small_Farm']).values[0]
    ]
})

    # Concatenate the new rows with the existing DataFrame
    # Insert new_rows_1 at the 6th index
    new_df = pd.concat([new_df.iloc[:6], new_rows_1, new_df.iloc[6:]], ignore_index=True)

    # Append new_rows_2 to the end of the DataFrame
    new_df = pd.concat([new_df, new_rows_2], ignore_index=True)

    # Calculate the difference between 'Agg Count' and 'Count'
    count_diff = new_df['Agg_Count'] - new_df['Count']

    # Insert the 'Count_Dif' column at position 2
    new_df.insert(2, 'Count_Dif', count_diff)

    # Calculate the percentages
    count_per = new_df['Count'] / new_df['Agg_Count']
    count_dif_per = new_df['Count_Dif'] / new_df['Agg_Count']
    agg_count_per = new_df['Agg_Count'] / new_df['Agg_Count']

    # Insert the percentage columns
    new_df.insert(2, 'Count_Per', count_per)
    new_df.insert(4, 'Count_Dif_Per', count_dif_per)
    new_df.insert(6, 'Agg_Count_Per', agg_count_per)
    new_df['Demographic'] = new_df['Demographic'].replace({'SB_Low': 'Low Income', 'SB_Mod': 'Moderate Income', 'SB_Total': 'All', 'All_Small': 'Less Than $1M', 'Low_Rev': '$1-$250k', 'Middle_Rev': '$250k-$1Mil', 'SB_1M': 'Over $1M', 'SF_Low': 'Low Income', 'SF_Mod': 'Moderate Income', 'SF_Total': 'All', 'SF_All_Small': 'Less Than $1M', 'SF_Low_Rev': '$1-$250k', 'SF_Middle_Rev': '$250k-$1Mil', 'SF_1M': 'Over $1M',})


    group_labels = ['Businesses Count by Income Tract'] * 3 + ['Business Size (By Revenue)'] * 4 + ['Farm Count By Income Tract'] * 3 + ['Farm Size (By Revenue)'] * 4
    new_df.insert(0, 'Group', group_labels)
    new_df = new_df.fillna(0)
    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style=2, color="blue")
    .tab_stub(rowname_col="Demographic", groupname_col="Group")
    .tab_header(title = "Assessment Area Business Demographics", subtitle=f"{selected_bank} in {selected_area}")
    .tab_spanner(label="Inside", columns=['Count', 'Count_Per'])
    .tab_spanner(label="Outside", columns=['Count_Dif', 'Count_Dif_Per'])
    .tab_spanner(label="Totals", columns=['Agg_Count', 'Agg_Count_Per'])
    .fmt_percent(columns=['Count_Per', 'Count_Dif_Per', 'Agg_Count_Per'], decimals=1)
    .fmt_number(columns=[ 'Count', 'Count_Dif', 'Agg_Count' ], decimals=0, use_seps=True)
    .cols_align(align="center", columns=[ 'Group','Demographic', 'Count', 'Count_Per', 'Count_Dif', 'Count_Dif_Per', 'Agg_Count', 'Agg_Count_Per'])
    .cols_label(Count = '#', Count_Per = '%', Count_Dif = '#',Count_Dif_Per = '%', Agg_Count = '#', Agg_Count_Per= '%')
    .tab_options(
        table_body_hlines_style="solid",
        table_body_vlines_style="solid",
        table_body_border_top_color="gray",
        table_body_border_bottom_color="gray",
        container_width="100%",
        stub_font_weight="bold",       # Make stub text bold
        stub_font_size="14px",         # Adjust stub font size if needed
        stub_background_color="lightgray",  # Set stub background color if desired
        stub_border_style="solid",     # Set stub border style
        stub_border_color="gray",      # Set stub border color
        row_group_font_weight="bold",      # Make row group labels bold
        row_group_font_size="16px",        # Adjust row group font size if needed
        row_group_background_color="lightblue",  # Set row group background color if desired
        row_group_padding="8px",           # Add padding around row group labels
    )
    .opt_vertical_padding(scale=1.5)
    .opt_horizontal_padding(scale=1.2)
    
)
    return gt_instance

def demographics_table(df, selected_bank, selected_area):
    df = df.sum()
    df = df.to_pandas()
    df = df.drop(['State_Code', 'County_Code'], axis=1)

    demo = ['OO_Low', '00_Mod', 'OO_Total', 'Units_Low', 'Units_Mod', 'Units_Total', 'Income_Low', 'Income_Mod','Family_Count']
    inside_counts = df[['Owner_Occupied_Units_TILow_Inside', 'Owner_Occupied_Units_TIMod_Inside', 'Owner_Occupied_Units_Inside', 'Total5orMoreHousingUnitsInStructure_TILow_Inside', 'Total5orMoreHousingUnitsInStructure_TIMod_Inside','Total5orMoreHousingUnitsInStructure_Inside','Low_Income_Family_Count_Inside','Moderate_Income_Family_Count_Inside','Family_Count_Inside'
    ]].values[0]
    total_counts = df[['Owner_Occupied_Units_TILow','Owner_Occupied_Units_TIMod','Owner_Occupied_Units','Total5orMoreHousingUnitsInStructure_TILow','Total5orMoreHousingUnitsInStructure_TIMod','Total5orMoreHousingUnitsInStructure','Low_Income_Family_Count','Moderate_Income_Family_Count','Family_Count',
    ]].values[0]

    # Create the new DataFrame
    new_df = pd.DataFrame({
        'Demographic': demo,
        'Count': inside_counts,
        'Agg_Count': total_counts,
    })

    # Calculate the difference between 'Agg Count' and 'Count'
    count_diff = new_df['Agg_Count'] - new_df['Count']

    # Insert the 'Count_Dif' column at position 2
    new_df.insert(2, 'Count_Dif', count_diff)

    # Calculate the percentages
    count_per = new_df['Count'] / new_df['Agg_Count']
    count_dif_per = new_df['Count_Dif'] / new_df['Agg_Count']
    agg_count_per = new_df['Agg_Count'] / new_df['Agg_Count']

    # Insert the percentage columns
    new_df.insert(2, 'Count_Per', count_per)
    new_df.insert(4, 'Count_Dif_Per', count_dif_per)
    new_df.insert(6, 'Agg_Count_Per', agg_count_per)

    group_labels = ['Owner Occupied Units'] * 3 + ['Housing With 5+ Units'] * 3 + ['Family Counts'] * 3
    new_df.insert(0, 'Group', group_labels)

    new_df['Demographic'] = new_df['Demographic'].replace({'OO_Low': 'Low Income', '00_Mod': 'Moderate Income', 'OO_Total': 'Total', 'Units_Low': 'Low Income', 'Units_Mod': 'Moderate Income', 'Units_Total': 'Total', 'Income_Low': 'Low Income', 'Income_Mod': 'Moderate Income', 'Family_Count': 'Total'})

    print(new_df)

    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style=2, color="blue")
    .tab_stub(rowname_col="Demographic", groupname_col="Group")
    .tab_style(
        style=style.text( weight = "bold"),
        locations=loc.body(rows=[2,5,8]),
    )
    .tab_style(
        style=style.fill(color="lightcyan"),
        locations=loc.body(rows=[2,5,8]),
    )
    .tab_header(title = "Assessment Area Residential Demographics", subtitle=f"{selected_bank} in {selected_area}")
    .tab_spanner(label="Inside", columns=['Count', 'Count_Per'])
    .tab_spanner(label="Outside", columns=['Count_Dif', 'Count_Dif_Per'])
    .tab_spanner(label="Totals", columns=['Agg_Count', 'Agg_Count_Per'])
    .fmt_percent(columns=['Count_Per', 'Count_Dif_Per', 'Agg_Count_Per'], decimals=1)
    .fmt_number(columns=[ 'Count', 'Count_Dif', 'Agg_Count' ], decimals=0, use_seps=True)
    .cols_align(align="center", columns=[ 'Group','Demographic', 'Count', 'Count_Per', 'Count_Dif', 'Count_Dif_Per', 'Agg_Count', 'Agg_Count_Per'])
    .cols_label(Count = '#', Count_Per = '%', Count_Dif = '#',Count_Dif_Per = '%', Agg_Count = '#', Agg_Count_Per= '%')
    .tab_options(
        table_body_hlines_style="solid",
        table_body_vlines_style="solid",
        table_body_border_top_color="gray",
        table_body_border_bottom_color="gray",
        container_width="100%",
        stub_font_weight="bold",       # Make stub text bold
        stub_font_size="14px",         # Adjust stub font size if needed
        stub_background_color="lightgray",  # Set stub background color if desired
        stub_border_style="solid",     # Set stub border style
        stub_border_color="gray",      # Set stub border color
        row_group_font_weight="bold",      # Make row group labels bold
        row_group_font_size="16px",        # Adjust row group font size if needed
        row_group_background_color="lightblue",  # Set row group background color if desired
        row_group_padding="8px",           # Add padding around row group labels
    )
    .opt_vertical_padding(scale=1.5)
    .opt_horizontal_padding(scale=1.2)
    
)
    return gt_instance


def overall_distribution_great_tables(df, selected_bank):
    df = df.sum()

    df = df.drop(['cra_current_year_assets',
		'cra_previous_year_assets',
		'hmda_assets',
		'number_of_branches',
		'bank_county_deposits',
		'Lender_in_CRA',
		'Lender_in_HMDA',
		'id_rssd',	
        'Partial_Ind',
		'MSA_Code',
		'MD_Code',
        'State_Code',
        'Loan_Orig_SFam_Closed',
        'Loan_Orig_SFam_Open',
        'Loan_Orig_MFam',
        'Loan_Orig_SFam_Closed_Inside', 
        'Loan_Orig_SFam_Open_Inside', 
        'Loan_Orig_MFam_Inside', 
        'SB_Loan_Orig_Inside', 
        'SF_Loan_Orig_Inside',
        'Amt_Orig_SFam_Closed_Inside',
        'Amt_Orig_SFam_Open_Inside',
        'Amt_Orig_MFam_Inside',
        'SB_Amt_Orig_Inside',
        'SF_Amt_Orig_Inside',
        'SB_Loan_Orig',
        'SF_Loan_Orig',
        'Loan_Orig',
        'County_Code'])

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
    
    
    df = df.transpose(include_header = True)


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
    .tab_header(title = "Loan Distribution", subtitle = f"{selected_bank}")
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


def overall_inside_out_great_table(df, selected_bank):
     
    df = df.drop(['cra_current_year_assets',
		'cra_previous_year_assets',
		'hmda_assets',
		'number_of_branches',
		'bank_county_deposits',
		'Lender_in_CRA',
        'Amt_Orig',
		'Lender_in_HMDA',
		'id_rssd',	
        'Partial_Ind',
		'MSA_Code',
		'MD_Code',
        'Loan_Orig',
        'State_Code',
        'County_Code'])
     
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

    group_labels = ['Residential'] * 4 + ['Business'] * 3 + ['Total'] * 1
    new_df.insert(0, 'Group', group_labels)

    new_df.columns = [
        'Group','a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'
    ]

    # Create a Great Tables instance with the new dataframe
    gt_instance = (
    GT(new_df)
    .opt_table_outline()
    .opt_stylize(style=2, color="blue")
    .tab_stub(rowname_col="a", groupname_col="Group")
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
    .tab_header(title = "Assessment Area Distribution", subtitle=f"{selected_bank}")
    .tab_spanner(label="Inside", columns=['b', 'h', 'c', 'i'])
    .tab_spanner(label="Outside", columns=['d', 'j', 'e', 'k'])
    .tab_spanner(label="Totals", columns=['f', 'l', 'g', 'm'])
    .fmt_percent(columns=['h', 'i', 'j', 'k', 'l', 'm'], decimals=1)
    .fmt_number(columns=[ 'b', 'c', 'd', 'e', 'f', 'g' ], decimals=0, use_seps=True)
    .cols_align(align="center", columns=[ 'Group','a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'])
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
        container_width="100%",
        stub_font_weight="bold",       # Make stub text bold
        stub_font_size="14px",         # Adjust stub font size if needed
        stub_background_color="lightgray",  # Set stub background color if desired
        stub_border_style="solid",     # Set stub border style
        stub_border_color="gray",      # Set stub border color
        row_group_font_weight="bold",      # Make row group labels bold
        row_group_font_size="16px",        # Adjust row group font size if needed
        row_group_background_color="lightblue",  # Set row group background color if desired
        row_group_padding="8px",           # Add padding around row group labels
    )
    .opt_vertical_padding(scale=1.5)
    .opt_horizontal_padding(scale=1.2)
    
)
    return gt_instance

def fetch_assessment_area(engine, selected_bank, selected_year):
    query = f"SELECT MD_Code, MSA_Code, State_Code, County_Code FROM Retail_Table WHERE id_rssd = (SELECT id_rssd FROM PE_Table WHERE bank_name = '{selected_bank}')"
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

def top_areas(engine, df, selected_bank, selected_year):
    print(df)

    columns_to_drop = ['cra_current_year_assets', 'cra_previous_year_assets', 'hmda_assets', 'number_of_branches', 
                       'bank_county_deposits', 'Lender_in_CRA', 'Lender_in_HMDA', 'id_rssd', 'Amt_Orig_SFam_Closed', 
                       'Amt_Orig_SFam_Open', 'Amt_Orig_MFam', 'Partial_Ind', 'Loan_Orig_SFam_Closed_Inside', 
                       'Loan_Orig_SFam_Open_Inside', 'Loan_Orig_MFam_Inside', 'SB_Loan_Orig_Inside', 
                       'SF_Loan_Orig_Inside', 'Amt_Orig_SFam_Closed_Inside', 'Amt_Orig_SFam_Open_Inside', 
                       'Amt_Orig_MFam_Inside', 'SB_Amt_Orig_Inside', 'SF_Amt_Orig_Inside', 'Loan_Orig_SFam_Closed', 
                       'Loan_Orig_SFam_Open', 'Loan_Orig_MFam', 'MSA_Code']
    df = df.drop(columns_to_drop)

    assessment_areas = fetch_assessment_area(engine, selected_bank, selected_year)

    area_series = pd.Series(assessment_areas)

    # Add the "Area" column to the DataFrame
    df = df.with_columns('Area', area_series)

    # Group by "Area" and sum the specified columns
    df = df.groupby('Area').agg([
        pl.col('Amt_Orig').sum(),
        pl.col('SB_Amt_Orig').sum(),
        pl.col('SF_Amt_Orig').sum(),
        pl.col('Loan_Orig').sum(),
        pl.col('SB_Loan_Orig').sum(),
        pl.col('SF_Loan_Orig').sum()
    ])

    print(df)