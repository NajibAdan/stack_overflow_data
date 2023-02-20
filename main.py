import requests
import configparser
import mysql.connector
import sys

def insert_row(mydb,question_id,tags,view_count,answer_count,score,created,is_answered):
    mycursor = mydb.cursor()
    insert_sql = f"""INSERT INTO `questions` (question_id,tags,view_count,answer_count,score,created,is_answered) 
                    VALUES ({question_id},'{' '.join(tags)}',{view_count},{answer_count},{score},'{created}','{is_answered}')"""
    mycursor.execute(insert_sql)
    mydb.commit()

conf = configparser.ConfigParser()
try:
    conf.read('secret_values.txt')
except Exception as e:
    print('Could not read the configuration file:' + str(e))
    sys.exit()

HOSTNAME = conf['CONFIG']['host']
USERNAME = conf['CONFIG']['user']
PASSWORD = conf['CONFIG']['password']
DATABASE = conf['CONFIG']['database']
mydb = mysql.connector.connect(
  host=HOSTNAME,
  user=USERNAME,
  password=PASSWORD,
  database=DATABASE
)

url = 'https://api.stackexchange.com/2.3/questions?fromdate=1669766400&order=desc&sort=activity&site=stackoverflow'

response = requests.get(url)
response = response.json()

for item in response['items']:
    question_id = item['question_id']
    tags = item['tags']
    view_count = item['view_count']
    answer_count = item['answer_count']
    score = item['score']
    created = item['creation_date']
    is_answered = item['is_answered']
    insert_row(mydb,question_id,tags,view_count,answer_count,score,created,is_answered)