import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Function used to load data from S3 bucket to staging tables mentioned in the copy_table_queries
    Parameters:
    - cur : cursor object
    - conn : connection object
    return type: none
    '''
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Function used to insert data into facts and dimension tables from the staging tables mentioned in the insert_table queries
    Parameters:
    - cur : cursor object
    - conn : connection object
    return type: none
    '''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main function:
    - reads config files to get the credentials and other parameters
    - establishes connection to database
    - Extracts data from S3 bucket into staging tables
    - Loads data into facts and dimension tables from staging table
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()