"""The Redshift staging copy and Data Insertion of facts and dimension tables takes place here."""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Execute the query for loading the staging table."""
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Execute the query for loading the fact and dimension tables."""
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Primary function responsible for handling database connections and executing all operations."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
