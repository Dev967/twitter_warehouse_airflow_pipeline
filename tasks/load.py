from airflow.decorators import task
from couchdb_handle import CouchHandle
from warehouse_handle import DataWarehouseHandle
from datetime import datetime,timezone
import uuid


@task(task_id="load")
def LoadToDW():
    ch = CouchHandle()
    wh = DataWarehouseHandle()
    for id in ch.db:
        doc = ch.db[id]
        try:    
            tweet = doc.get("tweet")
            user = doc.get("user")

            tweet_key = uuid.uuid4().int & (1<<64)-1
            user_key = uuid.uuid4().int & (1<<64)-1
            date_key = uuid.uuid4().int & (1<<64)-1
            tweet_fact_key= uuid.uuid4().int & (1<<64)-1

            #tweet
            tweet_data = {
                "id": tweet.get("id"),
                "key": tweet_key,
                "text": tweet.get("text"),
                "conversation_id": tweet.get("conversation_id"),
                "lang": tweet.get("lang"),
                "sensitive": tweet.get("possibly_sensitive"),
                "source": tweet.get("source"),
                "is_reply": tweet.get("in_reply_to_user_id")
            } 

            #user
            user_data={
                "id": user.get("id"),
                "key": user_key,
                "name": user.get("name"),
                "username": user.get("username"),
                "created_at": datetime.strptime(user.get("created_at"),"%Y-%m-%dT%H:%M:%S.000z"),
                "description": user.get("description"),
                "location": user.get("location"),
                "verified": user.get("verified")
            }
            
            # entities
            entities = tweet.get("entities")
            if(entities):
                mentions = entities.get("mentions")
                if mentions:
                    for mention in mentions:
                        mention_data = {
                            "key": uuid.uuid4().int & (1<<64)-1,
                            "tweet_key": tweet_key,
                            "username": mention.get("username"),
                            "user_id": mention.get("id")
                        }
                        wh.insert_mention(mention_data)


                urls = entities.get("urls")
                if urls:
                    for url in urls:
                        url_data = {
                            "key": uuid.uuid4().int & (1<<64)-1,
                            "tweet_key": tweet_key,
                            "url": url.get("unwound_url"),
                            "title": url.get("title"),
                            "description": url.get("description")
                        }
                        wh.insert_url(url_data)

                hashtags = entities.get("hashtags")
                if hashtags: 
                    for hashtag in hashtags:
                        hashtag_data = {
                            "key": uuid.uuid4().int & (1<<64)-1,
                            "tweet_key": tweet_key,
                            "tag": hashtag.get("tag")
                        }
                        wh.insert_hashtag(hashtag_data)
            
            #tweet-fact
            tweet_pmetric = tweet.get("public_metrics")
            user_pmetric = user.get("public_metrics")
            tweet_fact_data = {
                "key": tweet_fact_key,
                "num_comments": tweet_pmetric.get("reply_count"),
                "num_likes": tweet_pmetric.get("like_count"),
                "num_retweets": tweet_pmetric.get("retweet_count"),
                "num_hashtags": len(hashtags) if hashtags else 0,
                "num_mentions": len(mentions) if mentions else 0,
                "num_urls": len(urls) if urls else 0,
                "num_followers": user_pmetric.get("followers_count"),
                "num_following": user_pmetric.get("following_count"),
                "num_tweet_count": user_pmetric.get("tweet_count"),
                "date_key": date_key,
                "tweet_key": tweet_key,
                "user_key": user_key
            }
            wh.insert_date({
                "key": date_key,
                "date": datetime.strptime(tweet.get("created_at"),"%Y-%m-%dT%H:%M:%S.000z")
            })
            wh.insert_user(user_data)
            print("HERE TWEET_DATA", tweet_data)
            wh.insert_tweet(tweet_data)
            wh.insert_tweet_fact(tweet_fact_data)

            wh.commit()
        except BaseException as e:
            print("ERROR: ", e)
            print("TWEET_DATA", tweet_data)