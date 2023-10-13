"""This file lists some of the business questions addressed by the dataset, along with their respective answers."""
import configparser
import psycopg2
from sql_queries import example_questions


def questions(cur, conn):
    """Display the business question and executes its associated answer."""
    quer_no = 0
    for query in example_questions:
        quer_no +=1
        cur.execute(query)

        if quer_no == 1:
            print("What are the most famous artists based on songplays?")
        if quer_no == 2:
            print("What are the most famous songs based on songplays?")
        if quer_no == 3:
            print("Who are the most frequent users of Sparkify?")
        if quer_no == 4:
            print("Who are the most frequent FREE users of Sparkify?")
        if quer_no == 5:
            print("What are daily trend of songplays?") #PS : We only have data of November 2018 , hence I have aggregated by day , else I would have aggregated by month and year too.
        column_names = [desc[0] for desc in cur.description]
        print(column_names)
        for row in cur.fetchall():
            print(row)


def main():
    """Primary function responsible for handling database connections and executing all operations."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    questions(cur, conn)


    conn.close()

if __name__ == "__main__":
    main()
