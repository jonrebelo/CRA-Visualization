import base64
from io import StringIO

def generate_html_summary(summary_content):
    return f"""
    <html>
    <head>
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
    </head>
    <body>
        {summary_content}
    </body>
    </html>
    """

def download_link(object_to_download, download_filename, mime_type='text/html'):
    """
    Generates a download link for the given object.
    """
    if isinstance(object_to_download, str):
        object_to_download = object_to_download.encode()
    b64 = base64.b64encode(object_to_download).decode()
    href = f'<a href="data:{mime_type};base64,{b64}" download="{download_filename}">Download {download_filename}</a>'
    return href

def format_number(number):
    try:
        return f"${number * 1000:,.0f}"
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