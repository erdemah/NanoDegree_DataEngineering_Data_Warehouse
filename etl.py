import configparser
import psycopg2

from redshiftbuilder import RedshiftBuilder
from time import sleep
from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import create_table_queries, drop_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from s3 to Redshift as staging tables.
    :param cur: cursor
    :param conn: dwh connection object
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables to fact and dimension tables.
    :param cur: cursor
    :param conn: Redshift connection object
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """
    Drops tables one by one in the dwh within a for loop
    :param cur: cursor
    :param conn: dwh connection object
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables in the dwh for given queries in a list
    :param cur: cursor
    :param conn: dwh connection object
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connection to dwh is realised using the credentials in dwh.cfg file.
    Loads data from s3 to dwh as staging tables and
    inserts data from staging tables to fact and dimension tables.
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # config["IAM_ROLE"] = {"ARN":"ASDASD"}
    # print(config.get("IAM_ROLE", "ARN"))

    # initiate redshift class to build redshift cluster
    redshift_cluster = RedshiftBuilder(config)
    redshift_cluster.build_cluster()
    dbc_con_param = redshift_cluster.get_dbc_access_parameters()

    conn = psycopg2.connect(dbc_con_param)
    cur = conn.cursor()
    print("Dropping Tables")
    drop_tables(cur, conn)
    print("Creating Tables")
    create_tables(cur, conn)

    print("Loading Tables")
    load_staging_tables(cur, conn)
    print("Inserting Tables")
    insert_tables(cur, conn)

    conn.close()
    print("Tiering Down the cluster within 5 mins")
    sleep(300)
    redshift_cluster.clean_up_cluster()


if __name__ == "__main__":
    main()
