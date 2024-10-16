import pandas as pd
from scripts.postgres_helper import upload_overwrite_table
import csv

def upload_to_postgres(**kwargs):
    file_name=kwargs.get('file_name')
    table_name = file_name.split('.')[0]
    
    raw_df = pd.read_csv(
        f'dags/scripts/data_examples/{file_name}'
    )

    upload_overwrite_table(raw_df, table_name)

# Cleaning 'customer_reviews_google.csv'
def filter_and_clean_csv(file_path, output_path):
    cleaned_rows = []
    max_columns = 28
    
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file, delimiter=',', quotechar='"')
        
        header = next(csv_reader)
        cleaned_rows.append(header)
        
        # Loop through the remaining rows, applying the '0x' rule
        for row in csv_reader:
            # Filter rows where the first column starts with '0x'
            if len(row) > 0 and row[0].startswith('0x'):
                # Slicing
                if len(row) > max_columns:
                    cleaned_rows.append(row[:max_columns])
                else:
                    cleaned_rows.append(row)

    with open(output_path, 'w', encoding='utf-8', newline='') as output_file:
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(cleaned_rows)

    df = pd.read_csv(output_path)
    
    return df

input_google = 'dags/scripts/data_examples/customer_reviews_google.csv'
output_google = 'dags/scripts/data_examples/customer_reviews_google_filtered.csv'
google_df = filter_and_clean_csv(input_google, output_google)

input_google_maps = 'dags/scripts/data_examples/company_profiles_google_maps.csv'
output_google_maps = 'dags/scripts/data_examples/company_profiles_google_maps_filtered.csv'
google_maps_df = filter_and_clean_csv(input_google_maps, output_google_maps)
