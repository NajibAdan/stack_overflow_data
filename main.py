import configparser
import psycopg2
import sys
from scraper import Scraper

def main(conf):
    HOSTNAME = conf['CONFIG']['host']
    USERNAME = conf['CONFIG']['user']
    PASSWORD = conf['CONFIG']['password']
    DATABASE = conf['CONFIG']['database']
    API_KEY = conf['CONFIG']['key']

    conn = psycopg2.connect(dbname=DATABASE, user=USERNAME, password=PASSWORD,host=HOSTNAME)
    cur = conn.cursor()

    try:
        cur.execute('''
            CREATE TABLE questions
            (
            id            SERIAL PRIMARY KEY,
            question_id   Integer NOT NULL,
            tags          TEXT NOT NULL,
            view_count    Integer NOT NULL,
            answer_count  Integer NOT NULL,
            score         Integer NOT NULL,
            created       Integer NOT NULL,
            is_answered   Boolean NOT NULL
            )
        ''')
        print("Table Created successfully")
    except Exception as e:
        print(str(e))
        print('Table Creation failed. Probably the table exists')
    finally:
        conn.commit()
    scraper = Scraper(API_KEY,conn)
    scraper.scrap()

    print("Successfully loaded the data")
if __name__ == "__main__":
    conf = configparser.ConfigParser()
    try:
        conf.read('CONFIG_VALUES')
    except Exception as e:
        print('Could not read the configuration file:' + str(e))
        sys.exit()
    main(conf) 