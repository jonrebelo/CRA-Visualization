import streamlit as st
import io

def generate_html_export(html_content_dict, selected_bank, selected_year, selected_area):
    # Start HTML structure
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CRA Report for {selected_bank}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                width: 80%;
                margin: 0 auto;
            }}
            .header {{
                text-align: center;
                margin: 20px 0;
            }}
            .summary {{
                margin: 20px 0;
            }}
            .section {{
                margin: 20px 0;
            }}
            .summary-value {{
            font-weight: bold; /* Make summary value bold */
            }}
            .section-header {{
                font-size: 1.8em;
                font-weight: bold;
                margin-bottom: 10px;
            }}
            .disclaimer {{
                font-size: 0.9em;
                margin: 20px 0;
                border-top: 1px solid #ddd;
                padding-top: 10px;
                text-align: center;
                color: #555;
            }}
            .column-container {{
                display: flex;
                flex-wrap: wrap;
                gap: 20px;
                margin-bottom: 20px;
            }}
            .column {{
                flex: 1;
                min-width: 0;
                box-sizing: border-box;
                max-width: calc(50% - 20px);  /* Ensure only two columns */
            }}
            .column-1 {{
                margin-right: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Custom CRA Report for {selected_bank}</h1>
            </div>
            <div class="summary">
                <div class="summary-body">
                    <p><span class="summary-key">Year:</span> <span class="summary-value">{selected_year}</span></p>
                    <p><span class="summary-key">Location:</span> <span class="summary-value">{selected_area}</span></p>
                </div>
            </div>
            <div class="section">
                <div class="column-container">
    """
    
    # Split content into two columns
    items = list(html_content_dict.values())
    num_items = len(items)
    split_index = (num_items + 1) // 2  # Divide items into two columns
    
    # Add items to first column
    html_content += '<div class="column column-1">'
    for item in items[:split_index]:
        html_content += item + '<br>'
    html_content += '</div>'
    
    # Add items to second column
    html_content += '<div class="column column-1">'
    for item in items[split_index:]:
        html_content += item + '<br>'
    html_content += '</div>'
    
    # Close HTML structure
    html_content += """
                </div>
            </div>
            <div class="disclaimer">
                <p><strong>Disclaimer:</strong></p>
                <p>The banking and financial data used in this report is sourced from the <a href="https://www.federalreserve.gov/consumerscommunities/data_tables.htm" target="_blank">Federal Reserve</a>.</p>
                <p>Geographical data is sourced from the <a href="https://www.ffiec.gov/" target="_blank">FFIEC</a>.</p>
                <p>There may be slight deviations in the data due to different fiscal year timings and the fact that our data is separated by activity within the calendar year.</p>
                <p>While every effort is made to ensure accuracy, please verify any critical information with official sources or consult a financial expert.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Convert HTML content to bytes
    html_bytes = io.BytesIO(html_content.encode('utf-8'))

    # Use Streamlit's download button to trigger the download
    st.download_button(
        label="Download HTML Report",
        data=html_bytes,
        file_name=f"{selected_bank} custom cra_report.html",
        mime="text/html"
    )

def export_report(summary, loan_dist_html, inside_out_html, top_areas_html, top_bus_html, top_farm_html, selected_bank, selected_year):
    # Create HTML content
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CRA Report for {selected_bank} ({selected_year})</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                width: 80%;
                margin: 0 auto;
            }}
            .header {{
                text-align: center;
                margin: 20px 0;
            }}
            .summary {{
                margin: 20px 0;
            }}
            .section {{
                margin: 20px 0;
            }}
            .section-header {{
                font-size: 1.8em;
                font-weight: bold;
                margin-bottom: 10px;
            }}
            .disclaimer {{
                font-size: 0.9em;
                margin: 20px 0;
                border-top: 1px solid #ddd;
                padding-top: 10px;
                text-align: center;
                color: #555;
            }}
            .column-container {{
                display: flex;
                flex-wrap: wrap;
                gap: 20px;
                margin-bottom: 20px;
            }}
            .column {{
                flex: 1;
                min-width: 0;
                box-sizing: border-box;
            }}
            .column-1 {{
                flex: 1;
            }}
            .column-2 {{
                flex: 2;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>CRA Report for {selected_bank} ({selected_year})</h1>
            </div>
            <div class="summary">
                {summary}
            </div>
            <div class="section">
                <h2 class="section-header">Summarized Lending Splits</h2>
                <div class="column-container">
                    <div class="column column-1">
                        {loan_dist_html}
                    </div>
                    <div class="column column-2">
                        {inside_out_html}
                    </div>
                </div>
            </div>
            <div class="section">
                <h2 class="section-header">Lending Statistics by Loan Type</h2>
                <div class="column-container">
                    <div class="column column-1">
                        {top_areas_html}
                    </div>
                    <div class="column column-1">
                        {top_bus_html}
                    </div>
                    <div class="column column-1">
                        {top_farm_html}
                    </div>
                </div>
            </div>
            <div class="disclaimer">
                <p><strong>Disclaimer:</strong></p>
                <p>The banking and financial data used in this report is sourced from the <a href="https://www.federalreserve.gov/consumerscommunities/data_tables.htm" target="_blank">Federal Reserve</a>.</p>
                <p>Geographical data is sourced from the <a href="https://www.ffiec.gov/" target="_blank">FFIEC</a>.</p>
                <p>There may be slight deviations in the data due to different fiscal year timings and the fact that our data is separated by activity within the calendar year.</p>
                <p>While every effort is made to ensure accuracy, please verify any critical information with official sources or consult a financial expert.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    st.download_button(
        label="Export Report to HTML",
        data=html,
        file_name=f'{selected_bank}_{selected_year}_CRA_Report.html',
        mime='text/html'
    )


def format_number(number):
    try:
        return f"{number * 1000:,.0f}"
    except (TypeError, ValueError):
        return "N/A"
    

def group_and_sort_assessment_areas(assessment_areas):
    grouped_areas = {}
    for area in assessment_areas:
        state = area.split(',')[-1].strip()
        if state not in grouped_areas:
            grouped_areas[state] = []
        grouped_areas[state].append(area)
    
    for state in grouped_areas:
        grouped_areas[state].sort()
    
    sorted_areas = []
    for state in sorted(grouped_areas):
        sorted_areas.extend(grouped_areas[state])
    
    return sorted_areas


