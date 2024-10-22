import pandas as pd
import csv
import re

def company_profiles_clean(file_path, output_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        raw_data = file.read()

    cleaned_data = raw_data.replace('\\"', '&&')

    with open(output_path, 'w', encoding='utf-8') as output_file:
        output_file.write(cleaned_data)

input_maps = 'dags/scripts/data_examples/company_profiles_google_maps.csv'
output_maps = 'dags/scripts/data_examples/company_profiles_escaped.csv'
company_profiles_escaped = company_profiles_clean(input_maps, output_maps)

input_customer = 'dags/scripts/data_examples/customer_reviews_google.csv'
output_customer = 'dags/scripts/data_examples/customer_reviews_escaped.csv'
customer_reviews_escaped = company_profiles_clean(input_customer, output_customer)