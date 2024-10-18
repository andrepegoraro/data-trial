import pandas as pd
import csv
import re

def company_profiles_clean(file_path, output_path):
    cleaned_rows = []
    max_columns = 52
    
    pattern = re.compile(r'"\{.*?\}"')

    with open(file_path, 'r', encoding='utf-8') as file:
        raw_data = file.read()

    # Treating values that are between "{ and }"
    fixed_data = re.sub(pattern, lambda x: x.group(0).replace(',', ';;'), raw_data)

    with open('temp_fixed.csv', 'w', encoding='utf-8') as temp_file:
        temp_file.write(fixed_data)

    with open('temp_fixed.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file, delimiter=',', quotechar='"', skipinitialspace=True)
        
        header = next(csv_reader)
        cleaned_rows.append(header)

        for row in csv_reader:
            if len(row) > 0 and row[0].startswith('0x'):
                new_row = []
                for column in row:
                    new_row.append(column.replace(';;', ','))

                if len(new_row) > max_columns:
                    cleaned_rows.append(new_row[:max_columns])
                else:
                    cleaned_rows.append(new_row)

    with open(output_path, 'w', encoding='utf-8', newline='') as output_file:
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(cleaned_rows)

    df = pd.read_csv(output_path)
    
    return df

input_google_maps = 'dags/scripts/data_examples/company_profiles_google_maps.csv'
output_google_maps = 'dags/scripts/data_examples/company_profiles_google_maps_filtered.csv'
company_profiles_df = company_profiles_clean(input_google_maps, output_google_maps)