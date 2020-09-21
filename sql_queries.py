import configparser

#Config Files
config = configparser.ConfigParser()
config.read('redshift.cfg')

#Drop Table Queries
cases_table_drop = "DROP TABLE IF EXISTS covid_cases"
tests_table_drop = "DROP TABLE IF EXISTS covid_tests"
population_table_drop = "DROP TABLE IF EXISTS populations"
time_table_drop = "DROP TABLE IF EXISTS time"


#Create Table Queries
cases_table_create= ("""CREATE TABLE IF NOT EXISTS covid_cases(
                            date VARCHAR,
                            county VARCHAR,
                            state_full VARCHAR,
                            fips DOUBLE PRECISION,
                            cases BIGINT,
                            deaths BIGINT,
                            code VARCHAR                           
                                );""")

#Create Table Queries
tests_table_create= ("""CREATE TABLE IF NOT EXISTS covid_tests(
                            date BIGINT,
                            state_full VARCHAR,
                            positive DOUBLE PRECISION,
                            negative DOUBLE PRECISION,
                            death DOUBLE PRECISION,
                            total DOUBLE PRECISION,
                            hash VARCHAR,
                            dateChecked VARCHAR,
                            totalTestResults DOUBLE PRECISION,
                            fips BIGINT,
                            deathIncrease BIGINT,
                            hospitalizedIncrease BIGINT,
                            negativeIncrease BIGINT,
                            positiveIncrease BIGINT,
                            totalTestResultsIncrease BIGINT,
                            hospitalized DOUBLE PRECISION,
                            pending DOUBLE PRECISION                           
                                );""")

population_table_create= ("""CREATE TABLE IF NOT EXISTS populations( 
                                    Id varchar, 
                                    Id2 bigint, 
                                    County varchar, 
                                    state varchar, 
                                    pop_estimate_2018 bigint);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        date VARCHAR,
                        date_ts DATE,
                        day INT, 
                        week INT, 
                        month INT,
                        year INT,
                        weekday INT);
                    """)

#Copy Table Queries
copy_table_cases = ("""
    COPY covid_cases
    FROM 's3://covid-delta-lake/delta/cases/'
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
    """).format(config.get("IAM_ROLE", "ARN"))

copy_table_tests = ("""
    COPY covid_tests 
    FROM 's3://covid-delta-lake/delta/tests/'
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
    """).format(config.get("IAM_ROLE", "ARN"))

copy_table_population = ("""
    COPY populations 
    FROM 's3://covid-delta-lake/delta/populations/'
    IAM_ROLE '{}' 
    FORMAT AS PARQUET;
    """).format(config.get("IAM_ROLE", "ARN"))

#Create Time Table
time_table_insert = ("""
INSERT INTO time (
                date,
                date_ts, 
                day, 
                week, 
                month,
                year,
                weekday)
SELECT  date as date,
        TO_DATE(date, 'YYYY-MM-DD') as date_ts,
        EXTRACT(day FROM date_ts) as day,
        EXTRACT(week FROM date_ts) as week,
        EXTRACT(month FROM date_ts) as month,
        EXTRACT(year FROM date_ts) as year,
        EXTRACT(weekday FROM date_ts) as weekday
FROM(
  SELECT DISTINCT date
  FROM covid_cases
  WHERE date IS NOT null)
""")

drop_table_queries = [cases_table_drop, time_table_drop, tests_table_create, population_table_drop]
create_table_queries = [cases_table_create,tests_table_create, population_table_create, time_table_create]
copy_table_queries = [copy_table_cases, copy_table_tests, copy_table_population, time_table_insert]