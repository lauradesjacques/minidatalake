import duckdb
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_corona_data():
    # Paths
    corona_dir = os.path.join(os.path.dirname(__file__), '../../data/corona')
    db_path = os.path.join(os.path.dirname(__file__), '../../minilake.duckdb')

    # Connect to DuckDB
    db_path = os.path.abspath(db_path)
    logger.info(f"DuckDB file (db_path) is: {db_path}")
    con = duckdb.connect(db_path)

    try:
        # Ingest all corona CSVs
        logger.info("Corona data directory contents:")
        for f in os.listdir(corona_dir):
            logger.info(f"  {f}")

        for fname in os.listdir(corona_dir):
            if fname.endswith('.csv'):
                fpath = os.path.join(corona_dir, fname)
                logger.info(f'Ingesting {fpath}...')
                df = pd.read_csv(fpath)
                table_name = fname.replace('.csv', '').lower()
                temp_table = f"{table_name}_temp"
                
                # Create a temporary table with the new data
                con.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM df")
                
                # Check if the main table exists
                table_exists = con.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                """).fetchone()[0] > 0
                
                if table_exists:
                    # Get the primary key columns based on the table
                    if table_name == 'day_wise':
                        pk_columns = ['Date']
                    elif table_name == 'country_wise_latest':
                        pk_columns = ['Country/Region']
                    elif table_name == 'worldometer_data':
                        pk_columns = ['Country/Region']
                    elif table_name == 'usa_county_wise':
                        pk_columns = ['County', 'State', 'Date']
                    elif table_name == 'full_grouped':
                        pk_columns = ['Date', 'Country/Region']
                    elif table_name == 'covid_19_clean_complete':
                        pk_columns = ['Date', 'Country/Region', 'Province/State']
                    else:
                        pk_columns = ['Date', 'Country/Region']  # default fallback
                    
                    # Create a new table that combines existing and new data, removing duplicates
                    pk_conditions = ' AND '.join([f"t1.{col} = t2.{col}" for col in pk_columns])
                    con.execute(f"""
                        CREATE OR REPLACE TABLE {table_name}_new AS
                        SELECT DISTINCT *
                        FROM (
                            SELECT * FROM {table_name}
                            UNION ALL
                            SELECT * FROM {temp_table}
                        ) t1
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {temp_table} t2
                            WHERE {pk_conditions}
                            AND t1.rowid > t2.rowid
                        )
                    """)
                    
                    # Replace the old table with the new one
                    con.execute(f"DROP TABLE {table_name}")
                    con.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
                    logger.info(f'Table {table_name} updated with new data in DuckDB.')
                else:
                    # If table doesn't exist, just rename the temp table
                    con.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
                    logger.info(f'Table {table_name} created in DuckDB.')
                
                # Clean up temporary table
                con.execute(f"DROP TABLE IF EXISTS {temp_table}")

    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        raise
    finally:
        con.close()
        logger.info('Ingestion complete.')

def main():
    """Main function to be called by Airflow"""
    ingest_corona_data()

if __name__ == "__main__":
    main() 