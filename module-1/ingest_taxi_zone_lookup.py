from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
import click

def ingest_data(
        url: str,
        engine: Engine,
        target_table: str,
):
    # Load the entire CSV file into memory
    print(f"Downloading and loading CSV file from {url}...")
    df = pd.read_csv(url)
    df_count = len(df)
    print(f"Loaded {df_count} rows into memory.")
    
    # Create table and insert all data in one pass
    print(f"Inserting data into {target_table}...")
    df.to_sql(name=target_table, con=engine, if_exists="replace", index=False)

    # Verify counts
    query = f"SELECT count(1) FROM {target_table}"
    db_count = pd.read_sql(query, con=engine).iloc[0, 0]

    print(f"DataFrame row count: {df_count}")
    print(f"Database row count: {db_count}")

    if df_count == db_count:
        print("Verification successful: Row counts match.")
    else:
        print(f"Verification failed: Row counts do not match ({df_count} vs {db_count}).")

    print(f'done ingesting to {target_table}')


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user name')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--pg-target', default='zone_lookup', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, pg_target):
    engine: Engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    ingest_data(url=url,
                engine=engine,
                target_table=pg_target
    )

if __name__ == '__main__':
    main()
