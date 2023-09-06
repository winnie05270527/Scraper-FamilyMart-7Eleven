import pandas as pd
from sqlalchemy import create_engine
import os
import config
import logging
import googlemaps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_geocode(address, gmaps_client):
    try:
        geocode_result = gmaps_client.geocode(address)
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            return location.get('lat'), location.get('lng')
        else:
            return None, None
    except Exception as e:
        logger.error(f"An error occurred while geocoding: {e}")
        return None, None

def process_data(input_csv, company_name, gmaps_client):
    try:
        df = pd.read_csv(input_csv)
        address_column = '分公司地址'
        df['緯度'], df['經度'] = zip(*df[address_column].apply(get_geocode, gmaps_client=gmaps_client))
        df['公司'] = company_name
        return df
    except Exception as e:
        logger.error(f"An error occurred while processing data: {e}")
        return None

def write_to_database(df, db_url, table_name):
    try:
        engine = create_engine(db_url)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        engine.dispose()
    except Exception as e:
        print(f"Error writing data to table {table_name}: {e}")

if __name__ == "__main__":
    project_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_csv_seven = os.path.join(project_directory, 'data', 'address_seven.csv')
    input_csv_family = os.path.join(project_directory, 'data', 'address_family.csv')

    DB_HOST = config.DB_HOST
    DB_USER = config.DB_USER
    DB_PASSWORD = config.DB_PASSWORD
    DB_DATABASE = config.DB_DATABASE

    API_KEY = config.GMAP_API_KEY

    gmaps = googlemaps.Client(key=API_KEY)

    df_seven = process_data(input_csv_seven, 'seven', gmaps)
    df_family = process_data(input_csv_family, 'family', gmaps)

    merged_df = pd.concat([df_family, df_seven], ignore_index=True)
    merged_df = merged_df.drop(["公司統一編號", "公司名稱"], axis=1)
    merged_df["縣市"] = merged_df["分公司地址"].str[:3]

    filtered_df = merged_df.dropna(subset=['緯度', '經度'])

    db_url = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
    write_to_database(filtered_df, db_url,'location')