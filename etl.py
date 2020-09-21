import configparser
import psycopg2
from sql_queries import copy_table_queries, create_table_queries, drop_table_queries

host="******.******.us-west-2.redshift.amazonaws.com"
db_name="******"
user="******"
pw="******"
port= "******"

def create_tables(cur, conn):
    """
    First drops any tables that are currently in Redshift and then creates tables for covid_cases, covid_tests, populations, and time
    Takes arguments:
    - cur: cursor object that points to Redshift database
    - conn: psycopg2 connection to Redshift database
    Output:
    - Covid_cases, covid_tests, populations, and time tables created in Redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads Delta Lake data into covid_cases, covid_tests, populations, and time tables using queries in sql_queries.py
    Takes 2 arguments:
    - cur: cursor object that points to Redshift database
    - conn: psycopg2 connection to Redshift database
    Output:
    - covid_cases, covid_tests, populations, and time are populated with Delta Lake data
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def data_quality_check(tables, cur, conn):
    """
    Performs quality checks to ensure that data has been loaded in Redshift correctly

    Takes 3 arguments:
    - tables: a list of tables to check
    - cur: cursor object that points to Redshift database
    - conn: psycopg2 connection to Redshift database
    Output:
    - validates that data is loaded into Redshift database
    """
    for table in tables:

        cur.execute(f"select count(*) from {table}")
        result = cur.fetchall()
        if result[0][0] > 1:
            print("Data quality inspection passed")
        else:
            raise ValueError(f"QA failed for {table}: Table contained 0 rows")

def main():
    """
    Reads 'dwh.cfg' config file and to get connection parameters for Redshift database
    Creates connection to Redshift database
    Creates cursor object that points to Redshift database
    Executes create_tables, insert_tables, and data_quality_check functions, as defined above
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("config credentials read")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db_name, user, pw, port))
    cur = conn.cursor()
    print("connected")

    create_tables(cur, conn)
    print("successfully created redshift tables")

    insert_tables(cur, conn)
    print("successfully inserted tables")

    data_quality_check(["covid_tests", "covid_cases", "populations", "time"], cur, conn)
    print("completed data quality check")

    conn.close()


if __name__ == "__main__":
    main()