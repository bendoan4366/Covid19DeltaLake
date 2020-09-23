import configparser
import psycopg2
from sql_queries import copy_table_queries, create_table_queries, drop_table_queries

host="[*********************]"
db_name="[*********************]"
user="[*********************]"
pw="[*********************]"
port= "[*********************]"

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

def table_count_quality_check(tables, cur, conn):
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
            print(f"Data quality inspection passed for {table}")
        else:
            raise ValueError(f"QA failed for {table}: Table contained 0 rows. Please clear all Redshit tables and re-run script")
        conn.commit()

def time_table_quality_check(cur, conn):
    """
    Performs quality checks to ensure that data has been loaded in Redshift correctly

    Takes 3 arguments:
    - cur: cursor object that points to Redshift database
    - conn: psycopg2 connection to Redshift database
    Output:
    - validates that data is loaded into Redshift database
    """

    cur.execute(f"select MAX(year), MIN(year) from time")

    result = cur.fetchall()
    max_year = result[0][0]
    min_year = result[0][1]

    if max_year <= 2022:
        if min_year >= 2019:
            print(f"Datetime QA passed for time table. Year ranges are within acceptable range.")
        else:
            raise ValueError(f"QA failed for time check. The minimum year value returned was {min_year}, which is outside the time range")
    else:
        raise ValueError(f"QA failed for time check. The maximum year value returned was {max_year}, which is outside the time range")

    conn.commit()

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

    table_count_quality_check(["covid_tests", "covid_cases", "populations", "time"], cur, conn)
    time_table_quality_check(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()