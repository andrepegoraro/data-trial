import pandas as pd

def cleaning_customer_reviews(file_path, output_path):
    df_cleaned = pd.read_csv(file_path, delimiter=',', quotechar='"', on_bad_lines='skip')

    # Keeping only rows where 'google_id' starts with '0x'
    if 'google_id' in df_cleaned.columns:
        df_cleaned = df_cleaned[df_cleaned['google_id'].str.startswith('0x', na=False)]

    df_cleaned.to_csv(output_path, index=False)  # Saving without index column

    return df_cleaned

input_google = 'dags/scripts/data_examples/customer_reviews_escaped.csv'
output_google = 'dags/scripts/data_examples/customer_reviews_escaped_filtered.csv'
customer_reviews_df = cleaning_customer_reviews(input_google, output_google)
