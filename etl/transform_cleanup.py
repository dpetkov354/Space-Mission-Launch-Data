import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Load environment variables for PostgreSQL
pg_host = os.getenv("PGHOST")
pg_user = os.getenv("PGUSER")
pg_port = os.getenv("PGPORT")
pg_database = os.getenv("PGDATABASE")
pg_password = os.getenv("PGPASSWORD")

postgres_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"

# Create SQLAlchemy engine for PostgreSQL
engine = create_engine(postgres_url)

try:
    # Read data from raw_data table into a pandas DataFrame
    raw_data_query = "SELECT * FROM raw_data"
    raw_data = pd.read_sql(raw_data_query, engine)

    # Drop the 'unnamed' column
    raw_data.drop(columns=['unnamed'], inplace=True)

    # Extract the country from the location column
    raw_data['launch_country'] = raw_data['location'].str.extract(r',\s*([^,]+)$')

    # Substitute rows with null values in the 'price' column with 0
    raw_data['price'] = raw_data['price'].fillna(0)

    # Split the 'detail' column into 'rocket' and 'payload'
    raw_data[['rocket', 'payload']] = raw_data['detail'].str.split(' \| ', expand=True)

    # Remove rows with null values for the specified columns
    required_columns = ['id',
                        'organisation',
                        'location',
                        'launch_country',
                        'date',
                        'detail',
                        'rocket',
                        'payload',
                        'rocket_status',
                        'price',
                        'mission_status'
                        ]

    raw_data = raw_data.dropna(subset=required_columns)

    # Convert 'date' column to datetime format, ensuring it's parsed in UTC
    raw_data['date'] = pd.to_datetime(raw_data['date'], errors='coerce', utc=True)

    # Ensure 'price' column is converted to float
    raw_data['price'] = pd.to_numeric(raw_data['price'], errors='coerce')

    # Only drop rows where 'date' couldn't be properly converted
    raw_data = raw_data.dropna(subset=['date', 'price'])

    # Remove duplicate rows
    raw_data = raw_data.drop_duplicates()

    # Write the DataFrame to the SQL database
    try:
        with engine.connect() as connection:
            # Start a transaction
            with connection.begin():
                connection.execute(text("TRUNCATE TABLE cleaned_data;"))
                connection.commit()
                print("All rows in 'cleaned_data' have been truncated.")

                raw_data.to_sql('cleaned_data', engine, if_exists='append', index=False)
                print("Data cleaned and written to cleaned_data table successfully.")

                # Print the number of rows added
                print(f"{len(raw_data)} rows have been added to 'cleaned_data'.")

    except Exception as e:
        print(f"An error occurred: {e}")

finally:
    engine.dispose()
