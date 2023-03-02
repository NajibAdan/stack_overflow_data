import requests 
import time
class Scraper():
    def __init__(self,api,conn=None,url=None):
        self.API_KEY = api
        self.conn = conn
        if url is None:
            self.URL = f'https://api.stackexchange.com/2.3/questions?fromdate=1656633600&todate=1675209600&order=asc&sort=activity&site=stackoverflow&pagesize=100&key={self.API_KEY}&page='
        else:
            self.URL = url + f'&key={self.API_KEY}&page='
    def insert_row(self,question_id,tags,view_count,answer_count,score,created,is_answered):
        mycursor = self.conn.cursor()
        insert_sql = f"""INSERT INTO `questions` (question_id,tags,view_count,answer_count,score,created,is_answered) 
                        VALUES ({question_id},'{' '.join(tags)}',{view_count},{answer_count},{score},'{created}','{is_answered}')"""
        mycursor.execute(insert_sql)
        self.conn.commit()
    
    def write_to_file(data,filename):
        with open(filename,'a') as txt_file:
            txt_file.write(",".join(str(v) for v in data ) + '\n' )
    def get_data(url):
        response = requests.get(url)
        response = response.json()
        print(f"Fetching page number: {url.split('=')[-1]}")
        return response
    
    def scrap(self,filename=None):
        if self.conn:
            to = 'db'
        else:
            to = 'file'
            if not filename:
                filename = 'stackoverflow.csv'
        has_more = True 
        page = 1
        while has_more:
            response = Scraper.get_data(self.URL+str(page))
            for item in response['items']:
                question_id = item['question_id']
                tags = '|'.join(item['tags'])
                view_count = item['view_count']
                answer_count = item['answer_count']
                score = item['score']
                created = item['creation_date']
                is_answered = item['is_answered']
                if to == 'db':
                    self.insert_row(question_id,tags,view_count,answer_count,score,created,is_answered)
                elif to == 'file':
                    Scraper.write_to_file([question_id,tags,view_count,answer_count,score,created,is_answered],filename)
            has_more = response['has_more']
            if 'backoff' in response:
                sleep = response['backoff'] +0.5 # Just to add an extra time to sleep to avoid being banned
            else:
                sleep = 0.5
            print(f'Sleeping for {sleep}')
            time.sleep(sleep)
            page += 1