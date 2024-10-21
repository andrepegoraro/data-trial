import pandas as pd

def cleaning_customer_reviews(file_path, output_path):
    df = pd.read_csv(file_path, delimiter=',', quotechar='"', on_bad_lines='skip')
    columns_to_drop = [
        'review_text'
        , 'review_photo_ids'
        # , 'owner_answer'
        # , 'review_link'
    ]
    df_cleaned = df.drop(columns=[col for col in columns_to_drop if col in df.columns], axis=1)

    # Keep only rows where 'google_id' starts with '0x'
    if 'google_id' in df_cleaned.columns:
        df_cleaned = df_cleaned[df_cleaned['google_id'].str.startswith('0x', na=False)]

    df_cleaned.to_csv(output_path, index=False)  # Saving without index column

    return df_cleaned

input_google = 'dags/scripts/data_examples/customer_reviews_google.csv'
output_google = 'dags/scripts/data_examples/customer_reviews_google_filtered.csv'
customer_reviews_df = cleaning_customer_reviews(input_google, output_google)

input_google2 = 'dags/scripts/data_examples/customer_reviews_escaped.csv'
output_google2 = 'dags/scripts/data_examples/customer_reviews_escaped_filtered.csv'
customer_reviews_df2 = cleaning_customer_reviews(input_google2, output_google2)
