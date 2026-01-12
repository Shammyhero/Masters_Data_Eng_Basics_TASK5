from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import glob
import os
import requests
import pygeohash as pgh

# Constants
DATA_DIR = "/opt/airflow/data"
DEST_DIR = "/opt/airflow/destination"
MERGED_FILE = os.path.join(DATA_DIR, "staging_merged.parquet")
ENRICHED_FILE = os.path.join(DATA_DIR, "staging_enriched.parquet")
TRANSFORMED_FILE = os.path.join(DATA_DIR, "staging_transformed.parquet")
OUTPUT_PARQUET = os.path.join(DEST_DIR, "processed_restaurants.parquet")
OUTPUT_CSV = os.path.join(DEST_DIR, "processed_restaurants.csv")

@dag(
    dag_id="restaurant_etl_modular",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'restaurant', 'modular', 'parquet'],
    doc_md="""
    # Modular Restaurant ETL
    
    Broken down into:
    1. **Extract**: Merge CSVs to Parquet.
    2. **Enrich**: Geocode missing coordinates.
    3. **Transform**: Add Geohashes.
    4. **Load**: Save final Parquet/CSV.
    """
)
def restaurant_modular_pipeline():

    @task
    def extract_data():
        print(f"Scanning {DATA_DIR} for CSVs...")
        all_files = glob.glob(os.path.join(DATA_DIR, "part-*.csv"))
        if not all_files:
            raise FileNotFoundError("No 'part-*.csv' files found.")
            
        print(f"Found {len(all_files)} files.")
        df_list = [pd.read_csv(f) for f in all_files]
        full_df = pd.concat(df_list, ignore_index=True)
        
        print(f"Saving merged data ({len(full_df)} rows) to {MERGED_FILE}")
        full_df.to_parquet(MERGED_FILE, index=False)
        return MERGED_FILE

    @task
    def enrich_data(input_path: str):
        df = pd.read_parquet(input_path)
        
        try:
            api_key = Variable.get("opencage_api_key")
        except KeyError:
            raise ValueError("Variable 'opencage_api_key' not set.")

        def get_coordinates(row):
            if pd.isna(row['lat']) or pd.isna(row['lng']):
                query = f"{row.get('city', '')}, {row.get('country', '')}"
                url = "https://api.opencagedata.com/geocode/v1/json"
                params = {'q': query, 'key': api_key, 'limit': 1, 'no_annotations': 1}
                try:
                    res = requests.get(url, params=params, timeout=5).json()
                    if res['results']:
                        loc = res['results'][0]['geometry']
                        return pd.Series([loc['lat'], loc['lng']])
                except Exception as e:
                    print(f"Failed to geocode {query}: {e}")
            return pd.Series([row['lat'], row['lng']])

        mask = df['lat'].isnull() | df['lng'].isnull()
        if mask.any():
            print(f"Enriching {mask.sum()} rows...")
            df.loc[mask, ['lat', 'lng']] = df[mask].apply(get_coordinates, axis=1)
        
        print(f"Saving enriched data to {ENRICHED_FILE}")
        df.to_parquet(ENRICHED_FILE, index=False)
        return ENRICHED_FILE

    @task
    def transform_data(input_path: str):
        df = pd.read_parquet(input_path)
        
        print("Generating Geohashes...")
        def compute_geohash(row):
            try:
                if pd.notna(row['lat']) and pd.notna(row['lng']):
                    return pgh.encode(float(row['lat']), float(row['lng']), precision=4)
            except:
                pass
            return None

        df['geohash'] = df.apply(compute_geohash, axis=1)
        
        print(f"Saving transformed data to {TRANSFORMED_FILE}")
        df.to_parquet(TRANSFORMED_FILE, index=False)
        return TRANSFORMED_FILE

    @task
    def load_data(input_path: str):
        df = pd.read_parquet(input_path)
        
        print(f"Saving final Parquet to {OUTPUT_PARQUET}")
        df.to_parquet(OUTPUT_PARQUET, index=False)
        
        print(f"Saving final CSV to {OUTPUT_CSV}")
        df.to_csv(OUTPUT_CSV, index=False)

    # Define Workflow
    # Task 1 -> Task 2 -> Task 3 -> Task 4
    file_path_1 = extract_data()
    file_path_2 = enrich_data(file_path_1)
    file_path_3 = transform_data(file_path_2)
    load_data(file_path_3)

dag = restaurant_modular_pipeline()