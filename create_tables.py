import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


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
    The script is mainly used for testing by dropping and creating tables.
    The fact and dimension tables are created here.
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('DWH', 'host'), config.get("DWH", "DWH_DB"), config.get("DWH", "DWH_DB_USER"), config.get("DWH", "DWH_DB_PASSWORD"), config.get("DWH", "DWH_PORT")))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()