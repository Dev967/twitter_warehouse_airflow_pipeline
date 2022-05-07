from airflow.decorators import task
from stream import DataHandle
from couchdb_handle import CouchHandle
from datetime import datetime,timezone


def filterSource(text):
    ch = CouchHandle()
    x = text.split('Twitter for ')
    x = x[len(x)-1].split("Twitter")
    return x[len(x)-1]

@task(task_id="trasform")
def TransformData():
    ch = CouchHandle()
    for id in ch.db:
        doc = ch.db[id]
        source = filterSource(doc.get("tweet").get("source"))
        doc["tweet"]["source"] = source
        ch.db.save(doc)