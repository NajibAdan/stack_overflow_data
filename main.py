import configparser
import mysql.connector
import sys
from scraper import Scraper

def main(conf):
    HOSTNAME = conf['CONFIG']['host']
    USERNAME = conf['CONFIG']['user']
    PASSWORD = conf['CONFIG']['password']
    DATABASE = conf['CONFIG']['database']
    API_KEY = conf['CONFIG']['key']

    conn = mysql.connector.connect(
    host=HOSTNAME,
    user=USERNAME,
    password=PASSWORD,
    database=DATABASE
    )
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