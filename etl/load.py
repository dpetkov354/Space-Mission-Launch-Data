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
    # Read data from cleaned_data table into a pandas DataFrame
    cleaned_data_query = "SELECT * FROM cleaned_data"
    cleaned_data = pd.read_sql(cleaned_data_query, engine)

    # Extract the country from the location column
    cleaned_data['launch_country'] = cleaned_data['location'].str.extract(r',\s*([^,]+)$')

    # Split the 'detail' column into 'rocket' and 'payload'
    cleaned_data[['rocket', 'payload']] = cleaned_data['detail'].str.split(' \| ', expand=True)

    # Write the DataFrame to the SQL database
    try:
        with engine.connect() as connection:
            # Start a transaction
            with connection.begin():
                connection.execute(text("TRUNCATE TABLE aggregate_data;"))
                connection.commit()
                print("All rows in 'aggregate_data' have been truncated.")

                cleaned_data.to_sql('aggregate_data', engine, if_exists='append', index=False)
                print("Data aggregate and written to aggregate_data table successfully.")

                # Print the number of rows added
                print(f"{len(cleaned_data)} rows have been added to 'aggregate_data'.")

    except Exception as e:
        print(f"An error occurred: {e}")

finally:
    engine.dispose()