import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Query executed successfully:", query)
        except Exception as e:
            print("Failed to execute query:", query)
            print("Error:", e)


def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Query executed successfully:", query)
        except Exception as e:
            print("Failed to execute query:", query)
            print("Error:", e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get('CLUSTER', 'HOST'),
        config.get('CLUSTER', 'DB_NAME'),
        config.get('CLUSTER', 'DB_USER'),
        config.get('CLUSTER', 'DB_PASSWORD'),
        config.get('CLUSTER', 'DB_PORT')
    ))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()