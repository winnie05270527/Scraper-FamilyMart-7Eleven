import pandas as pd
from sqlalchemy import create_engine
import os
import config

def read_excel_file(file_path):
    try:
        return pd.read_excel(file_path)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def write_to_database(df, db_url, table_name):
    try:
        engine = create_engine(db_url)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        engine.dispose()
    except Exception as e:
        print(f"Error writing data to table {table_name}: {e}")

if __name__ == "__main__":
    
    DB_HOST = config.DB_HOST
    DB_USER = config.DB_USER
    DB_PASSWORD = config.DB_PASSWORD
    DB_DATABASE = config.DB_DATABASE
    
    project_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path_seven = os.path.join(project_directory, 'data', 'revenue_seven.xlsx')
    file_path_family = os.path.join(project_directory, 'data', 'revenue_family.xlsx')

    df_seven = read_excel_file(file_path_seven)
    df_family = read_excel_file(file_path_family)
    
    db_url = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
    write_to_database(df_seven, db_url, 'revenue_seven')
    write_to_database(df_family, db_url, 'revenue_family')
