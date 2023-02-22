import requests
import configparser
import mysql.connector
import sys
import time

def insert_row(mydb,question_id,tags,view_count,answer_count,score,created,is_answered):
    mycursor = mydb.cursor()
    insert_sql = f"""INSERT INTO `questions` (question_id,tags,view_count,answer_count,score,created,is_answered) 
                    VALUES ({question_id},'{' '.join(tags)}',{view_count},{answer_count},{score},'{created}','{is_answered}')"""
    mycursor.execute(insert_sql)
    mydb.commit()

conf = configparser.ConfigParser()
try:
    conf.read('CONFIG_VALUES')
except Exception as e:
    print('Could not read the configuration file:' + str(e))
    sys.exit()

HOSTNAME = conf['CONFIG']['host']
USERNAME = conf['CONFIG']['user']
PASSWORD = conf['CONFIG']['password']
DATABASE = conf['CONFIG']['database']
API_KEY = conf['CONFIG']['key']

mydb = mysql.connector.connect(
  host=HOSTNAME,
  user=USERNAME,
  password=PASSWORD,
  database=DATABASE
)

url = f'https://api.stackexchange.com/2.3/questions?fromdate=1669766400&order=desc&sort=activity&site=stackoverflow&pagesize=100&key={API_KEY}&page='

print(url)
has_more = True
page = 1
while has_more:
    response = requests.get(url+str(page))
    response = response.json()
    print(f"Fetching page number: {page}")
    for item in response['items']:
        question_id = item['question_id']
        tags = '|'.join(item['tags'])
        view_count = item['view_count']
        answer_count = item['answer_count']
        score = item['score']
        created = item['creation_date']
        is_answered = item['is_answered']
        insert_row(mydb,question_id,tags,view_count,answer_count,score,created,is_answered)
    has_more = response['has_more']
    if 'backoff' in response:
        sleep = response['backoff']
    else:
        sleep = 0.5
    print(f'Sleeping for {sleep}')
    time.sleep(sleep)

print("Successfully loaded the data")