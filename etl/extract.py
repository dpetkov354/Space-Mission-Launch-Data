import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from kaggle.api.kaggle_api_extended import KaggleApi

# Load environment variables
load_dotenv()

# Set up Kaggle API
api = KaggleApi()
api.authenticate()

# Download the dataset
dataset = "sefercanapaydn/mission-launches"  # Replace with the actual dataset name
destination = "../raw_dataset/"
api.dataset_download_files(dataset, path=destination, unzip=True)
print(f"Dataset downloaded and unzipped to {destination}")

# Load environment variables for PostgreSQL
pg_host = os.getenv("PGHOST")
pg_user = os.getenv("PGUSER")
pg_port = os.getenv("PGPORT")
pg_database = os.getenv("PGDATABASE")
pg_password = os.getenv("PGPASSWORD")

postgres_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"

# Create SQLAlchemy engine for PostgreSQL
engine = create_engine(postgres_url)

csv_file_path = "../raw_dataset/mission_launches.csv"

# Load CSV into a Pandas DataFrame
df = pd.read_csv(csv_file_path)

# Rename columns to match SQL table schema
df.columns = ['id', 'unnamed', 'organisation', 'location', 'date', 'detail', 'rocket_status', 'price', 'mission_status']

# Write the DataFrame to the SQL database
try:
    with engine.connect() as connection:
        # Start a transaction
        with connection.begin():
            # Deleting all rows in the table do a rewrite
            connection.execute(text("DELETE FROM raw_data;"))
            print("All rows in 'raw_data' have been deleted.")

            # Insert the new data
            df.to_sql('raw_data', con=connection, if_exists='append', index=False)
            print("New data inserted into 'raw_data'.")

            # Print the number of rows inserted
            rows_inserted = df.shape[0]
            print(f"Number of rows inserted: {rows_inserted}")
except Exception as e:
    print(f"Error inserting data into SQL database: {e}")
