import pandas as pd
import os
import re


def load_csv(path, file):
    # Get the list of all files in a common directory
    path = '../PI01-Data-Engineering/Datasets/'
    try:
        df = pd.read_csv(path+file, sep=',')
    except UnicodeError:
        df = pd.read_csv(path+file, encoding='utf-16le', sep=',')
    df = df.to_csv('../PI01-Data-Engineering/Datasets_for_ETL/'+'df_'+file, index=False)
    print(f'Succesfull load of {file}')


def etl(path_ETL, file_ETL):
    path_ETL = '../PI01-Data-Engineering/Datasets_for_ETL/'
    dir_list_ETL = os.listdir(path_ETL)
    
    for files_etl in dir_list_ETL:
        if re.search(r'amazon', files_etl):
            amazon = pd.read_csv(path_ETL+files_etl)
            amazon['id'] = 'a' + amazon['show_id']
            amazon['rating'].fillna('G', inplace=True)
            amazon['date'] = pd.to_datetime(amazon['date_added'])
            for i in amazon.select_dtypes(include='object'):
                amazon[i] = amazon[i].str.lower()
            duration = amazon['duration'].str.split(" ", n=1, expand=True)
            amazon['duration_int'] = duration[0]
            amazon['duration_type'] = duration[1]
            amazon['duration_type'] = amazon['duration_type'].str.replace('seasons', 'season')
            amazon['duration_int'] = amazon['duration_int'].astype(int)
            amazon.fillna(value=0, inplace=True)
            amazon = amazon.to_csv('../PI01-Data-Engineering/PI_01/amazon.csv', index=False)
        elif re.search(r'disney', files_etl):
            disney = pd.read_csv(path_ETL+files_etl)
            disney['id'] = 'd' + disney['show_id']
            disney['rating'].fillna('G', inplace=True)
            disney['date'] = pd.to_datetime(disney['date_added'])
            for i in disney.select_dtypes(include='object'):
                disney[i] = disney[i].str.lower()
            duration = disney['duration'].str.split(" ", n=1, expand=True)
            disney['duration_int'] = duration[0]
            disney['duration_type'] = duration[1]
            disney['duration_type'] = disney['duration_type'].str.replace('seasons', 'season')
            disney['duration_int'] = disney['duration_int'].astype('Int32')
            disney.fillna(value=0, inplace=True)
            disney = disney.to_csv('../PI01-Data-Engineering/PI_01/disney.csv', index=False)
        elif re.search(r'hulu', files_etl):
            hulu = pd.read_csv(path_ETL+files_etl)
            hulu['id'] = 'h' + hulu['show_id']
            hulu['rating'].fillna('G', inplace=True)
            hulu['date'] = pd.to_datetime(hulu['date_added'])
            for i in hulu.select_dtypes(include='object'):
                hulu[i] = hulu[i].str.lower()
            duration = hulu['duration'].str.split(" ", n=1, expand=True)
            hulu['duration_int'] = duration[0]
            hulu['duration_type'] = duration[1]
            hulu['duration_type'] = hulu['duration_type'].str.replace('seasons', 'season')
            hulu['duration_int'] = hulu['duration_int'].astype('Int32')
            hulu.fillna(value=0, inplace=True)
            hulu = hulu.to_csv('../PI01-Data-Engineering/PI_01/hulu.csv', index=False)
        elif re.search(r'netflix', files_etl):
            netflix = pd.read_csv(path_ETL+files_etl)
            netflix['id'] = 'n' + netflix['show_id']
            netflix['rating'].fillna('G', inplace=True)
            netflix['date'] = pd.to_datetime(netflix['date_added'])
            for i in netflix.select_dtypes(include='object'):
                netflix[i] = netflix[i].str.lower()
            duration = netflix['duration'].str.split(" ", n=1, expand=True)
            netflix['duration_int'] = duration[0]
            netflix['duration_type'] = duration[1]
            netflix['duration_type'] = netflix['duration_type'].str.replace('seasons', 'season')
            netflix['duration_int'] = netflix['duration_int'].astype('Int32')
            netflix.fillna(value=0, inplace=True)
            netflix = netflix.to_csv('../PI01-Data-Engineering/PI_01/netflix.csv', index=False)
                    
# TODO
# def generating_id():
    # return id

# def rating_null_values():
    # amazon['rating'].fillna('G', inplace=True)

# def dates_formating()
    # return

# def lower_cases()
#     return

# def split_duration_column():
    # return split_columns


# def concat_df(path_ETL, file_ETL):
#     path_ETL = '../PI01-Data-Engineering/Datasets_for_ETL/'
#     dir_list_ETL = os.listdir(path_ETL)

#     df_full = pd.DataFrame()
#     for file_etl in dir_list_ETL:
#         df_data = pd.read_csv(path_ETL+file_etl)
#         df_full = pd.concat([df_full, df_data], ignore_index=True)
#         # Dropping duplicates
#         # Dropping NaN values
#     df_full = df_full.to_csv('../PI01-Data-Engineering/PI_01/df_full.csv', index=False)
#     print(f'Succesful load of {file_ETL}')

def concat_df(path, dir):
    path = path
    dir = os.listdir(path)

    df_full = pd.DataFrame()
    for file in dir:
        if re.search(r'.csv', file):
            df_data = pd.read_csv(path+file)
            df_full = pd.concat([df_full, df_data], ignore_index=True)
            print(f'Succesful load of {file}')
    df_full = df_full.to_csv('../PI01-Data-Engineering/PI_01/df_full.csv', index=False)


# for cv in dir_list_clean:
#     print(path_df_clean+cv)