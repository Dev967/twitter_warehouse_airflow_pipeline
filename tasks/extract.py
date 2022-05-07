from airflow.decorators import task
from stream import DataHandle
from couchdb_handle import CouchHandle
from datetime import datetime,timezone,timedelta


@task(task_id="extract")
def LoadToStaging():
    dh = DataHandle()
    ch = CouchHandle()

    now = datetime.today()
    date_start = now - timedelta(days=7)
    date_start = date_start.strftime("%Y-%m-%dT%H:%M:%S.000z")
    
    date_end = now - timedelta(days=4)
    date_end = date_end.strftime("%Y-%m-%dT%H:%M:%S.000z")

    url = dh.url_builder("darknet", date_start, date_end,"100", None)
    data = dh.get_tweets(url)

    next_token = None
    hasNext = True

    while(hasNext):
        try:    
            url = dh.url_builder("darknet", date_start, date_end,"10", next_token)
            data = dh.get_tweets(url)
            next_token = data['meta'].get("next_token")
            if(not next_token): hasNext = False

            tweets = data['data']
            users = data['includes']['users']

            for idx, tweet in enumerate(tweets):
                ch.insert({
                    "tweet": tweet,
                    "user": users[idx]
                })
        except BaseException as e:
            print("ERRORS: ", e)