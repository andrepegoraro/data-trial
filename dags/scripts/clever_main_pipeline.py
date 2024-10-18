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

def filter_and_clean_csv(file_path, output_path):
    cleaned_rows = []
    max_columns = 28
    
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file, delimiter=',', quotechar='"')
        
        header = next(csv_reader)
        cleaned_rows.append(header)
        
        for row in csv_reader:
            if len(row) > 0 and row[0].startswith('0x'):
                if len(row) > max_columns:
                    cleaned_rows.append(row[:max_columns])
                else:
                    cleaned_rows.append(row)

    with open(output_path, 'w', encoding='utf-8', newline='') as output_file:
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(cleaned_rows)

    df = pd.read_csv(output_path)
    
    return df

input_google_maps = 'dags/scripts/data_examples/company_profiles_google_maps.csv'
output_google_maps = 'dags/scripts/data_examples/company_profiles_google_maps_filtered.csv'
google_maps_df = filter_and_clean_csv(input_google_maps, output_google_maps) 

def cleaning_customer_reviews(file_path, output_path):
    df = pd.read_csv(file_path, delimiter=',', quotechar='"', on_bad_lines='skip')
    columns_to_drop = ['review_text', 'review_photo_ids', 'owner_answer', 'review_link']
    df_cleaned = df.drop(columns=[col for col in columns_to_drop if col in df.columns], axis=1)

    # Keep only rows where 'google_id' starts with '0x'
    if 'google_id' in df_cleaned.columns:
        df_cleaned = df_cleaned[df_cleaned['google_id'].str.startswith('0x', na=False)]

    df_cleaned.to_csv(output_path, index=False)  # Saving without index column

    return df_cleaned

input_google = 'dags/scripts/data_examples/customer_reviews_google.csv'
output_google = 'dags/scripts/data_examples/customer_reviews_google_filtered.csv'
cleaned_df_fixed = cleaning_customer_reviews(input_google, output_google)
