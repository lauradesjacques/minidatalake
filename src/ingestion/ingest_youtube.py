import duckdb
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_youtube_data():
    # Paths
    youtube_dir = os.path.join(os.path.dirname(__file__), '../../data/youtube')
    db_path = os.path.join(os.path.dirname(__file__), '../../minilake.duckdb')

    # Connect to DuckDB
    db_path = os.path.abspath(db_path)
    con = duckdb.connect(db_path)

    try:
        # Ingest all YouTube CSVs
        for fname in os.listdir(youtube_dir):
            if fname.endswith('.csv'):
                fpath = os.path.join(youtube_dir, fname)
                logger.info(f'Ingesting {fpath}...')
                try:
                    df = pd.read_csv(fpath, encoding='utf-8')
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(fpath, encoding='latin1')
                        logger.warning(f"Warning: {fname} was read with 'latin1' encoding.")
                    except Exception as e:
                        logger.error(f'Failed to ingest {fname}: {e}')
                        continue
                
                table_name = f"youtube_{fname.replace('.csv', '').lower()}"
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
                    # For YouTube data, video_id is the primary key
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
                            WHERE t1.video_id = t2.video_id
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
        logger.info('YouTube ingestion complete.')

def main():
    """Main function to be called by Airflow"""
    ingest_youtube_data()

if __name__ == "__main__":
    main() 