import mysql.connector
import auth


class DataWarehouseHandle:
    def __init__(self):
        self.db = mysql.connector.connect(user=auth.creds["WH_USER"], host=auth.creds["WH_HOST"], password=auth.creds["WH_PASSWORD"], database=auth.creds["WH_DBNAME"])
        self.cursor = self.db.cursor()
    
    def insert_tweet(self, tweet_data):
        sql_query = (
                    "INSERT INTO tweet_dim "
                    "VALUES(%(id)s, %(key)s, %(text)s, %(conversation_id)s, %(is_reply)s, %(lang)s, %(sensitive)s, %(source)s)"
                    )
        self.cursor.execute(sql_query, tweet_data)
    
    def insert_user(self,user_data):
        sql_query = (
                    "INSERT INTO user_dim "
                    "VALUES(%(id)s, %(key)s, %(name)s, %(username)s, %(description)s, %(location)s, %(verified)s, %(created_at)s)"
                    )
        self.cursor.execute(sql_query, user_data)
    
    def insert_date(self, date_data):
        sql_query = (
                    "INSERT INTO date_dim "
                    "VALUES(%(key)s, %(date)s)"
                    )
        self.cursor.execute(sql_query, date_data)
    
    def insert_hashtag(self, hashtag_data):
        sql_query = (
                    "INSERT INTO hashtag_dim "
                    "VALUES(%(key)s, %(tweet_key)s, %(tag)s)"
                    )
        self.cursor.execute(sql_query, hashtag_data)

    def insert_mention(self, mention_data):
        sql_query = (
                    "INSERT INTO mention_dim "
                    "VALUES(%(key)s, %(tweet_key)s, %(username)s, %(user_id)s)"
                    )
        self.cursor.execute(sql_query, mention_data)
    
    def insert_url(self, url_data):
        sql_query = (
                    "INSERT INTO url_dim "
                    "VALUES(%(key)s, %(tweet_key)s, %(url)s, %(description)s, %(title)s)"
                    )
        self.cursor.execute(sql_query, url_data)

    def insert_tweet_fact(self, tweet_fact_data):
        sql_query = (
                    "INSERT INTO tweet_fact "
                    "VALUES(%(key)s, %(num_comments)s, %(num_likes)s, %(num_retweets)s, %(num_hashtags)s, %(num_mentions)s, %(num_urls)s, %(num_followers)s, %(num_following)s, %(num_tweet_count)s, %(date_key)s, %(tweet_key)s, %(user_key)s)"
                    )
        self.cursor.execute(sql_query, tweet_fact_data) 
    
    def commit(self):
        self.db.commit()