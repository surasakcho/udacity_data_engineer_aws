import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, count_rows_queries, insight_queries


def load_staging_tables(cur, conn):
    print('Loading staging tables...')
    for query in copy_table_queries:
        print(f'Executing query...\n"{query}"')
        cur.execute(query)
        conn.commit()
        print('Done.')


def insert_tables(cur, conn):
    print('Inserting tables...')
    for query in insert_table_queries:
        print(f'Executing query...\n"{query}"')
        cur.execute(query)
        conn.commit()
        print('Done.')

def count_rows(cur, conn):
    print('Counting rows from each table...')
    for query in count_rows_queries:
        print(f'Executing query...\n"{query}"')
        cur.execute(query)
        rows = cur.fetchall()
        print(rows) 
        print("----------")
        print()


def insight(cur, conn):
    print('Finding insight from data...')
    for query in insight_queries:
        print(f'Executing query...\n"{query}"')
        cur.execute(query)
        rows = cur.fetchall()
        print(rows) 
        print("----------")
        print()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
#     load_staging_tables(cur, conn)
#     insert_tables(cur, conn)
    count_rows(cur, conn)
    insight(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()