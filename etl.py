import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_counts_queries, questions_dict

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

key = config.get('AWS', 'KEY')
secret = config.get('AWS', 'secret')
roleArn = config.get('IAM_ROLE', 'ARN')
log_data = config.get('S3', 'log_data')
song_data = config.get('S3', 'song_data')
log_jsonpath = config.get('S3', 'log_jsonpath')

            
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Query executed successfully:", query.split()[0:2])
        except Exception as e:
            print("Failed to execute query:", query.split()[0:2])
            print("Error:", e)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Query executed successfully:", query)
        except Exception as e:
            print("Failed to execute query:", query)
            print("Error:", e)
            
def table_counts(cur, conn):
    for key, value in questions_dict.items():
        if key.startswith('question'):
            print(value)  # Print the question
        elif key.startswith('query'):
            cur.execute(value)  # Execute the query
            result = cur.fetchall()  # Fetch all rows
            print(result)  # Print the fetchall() result
    
            
def questions_and_answers(cur, conn):
    for key, value in questions_dict.items():
        if key.startswith('question'):
            print(value)  # Print the question
        elif key.startswith('query'):
            cur.execute(value)  # Execute the query
            result = cur.fetchall()  # Fetch all rows
            if result:
                print(result)  # Print the fetchall() result
        
        
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    table_counts(cur, conn)
    questions_and_answers(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()