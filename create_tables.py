"""The Redshift tables creation,from staging to fact and dimension tables."""
import configparser
import psycopg2
import boto3
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries




def drop_tables(cur, conn):
    """Execute the query for dropping the staging,fact and dimension tables."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Execute the query for creating the staging,fact and dimension tables."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Primary function responsible for handling database connections and executing all operations."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config.read_file(open('dwh.cfg'))
    print(*config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
