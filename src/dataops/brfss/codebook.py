import re

import pandas as pd
from bs4 import BeautifulSoup

# TODO add function for download codebook with requests
# TODO handle zip and clean up
# TODO functionalize the below

# local test file: USCODE23_LLCP_021924.HTML

# Load the HTML file
with open("<FILEHERE>", "r") as f:
    contents = f.read()

# Create a BeautifulSoup object to parse the HTML
soup = BeautifulSoup(contents, "html.parser")

# Find all tables in the document
tables = soup.find_all(
    "table", {"class": "table", "summary": "Procedure Report: Report"}
)

# Lists to store extracted data
labels = []
values = []
value_labels = []
column_texts = []

# Loop through each table to extract data
for table in tables:
    # Extract the question label and column text from the first row
    header_content = table.find("td", class_="linecontent")
    if header_content:
        header_text = header_content.get_text(separator="\n")

        # Extract the question label
        label_match = re.search(r"Label:(.*?)\n", header_text)
        question_label = label_match.group(1).strip() if label_match else "N/A"

        # Extract the column text
        column_match = re.search(r"Column:(.*?)\n", header_text)
        column_text = column_match.group(1).strip() if column_match else "N/A"
    else:
        continue

    # Find all data rows in the table body
    data_rows = table.find("tbody").find_all("tr") if table.find("tbody") else []

    # Loop through each data row
    for row in data_rows:
        cols = row.find_all(["td", "th"], class_="data")
        if not cols:
            cols = row.find_all("td", class_=lambda x: x != "l m linecontent")

        if len(cols) >= 2:
            # Extract the value and value label from the appropriate columns
            value = cols[0].text.strip()
            value_label = cols[1].text.strip()

            # Append the extracted data to the lists
            labels.append(question_label)
            values.append(value)
            value_labels.append(value_label)
            column_texts.append(column_text)

# Create a pandas DataFrame from the extracted data
df = pd.DataFrame(
    {
        "Label": labels,
        "Value": values,
        "Value Label": value_labels,
        "Column Text": column_texts,
    }
)

# Display the resulting DataFrame
print(df)
