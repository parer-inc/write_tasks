"""This service allows to write new tasks to db"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()

def write_tasks(data):
    """Write tasks into database (table tasks)
       data must be a 1d array of video ids"""
    cursor, db = get_cursor()
    if not cursor or not db:
        # log that failed getting cursor
        return False
    data2 = []
    for id in data:
        data2.append([id])
    try:
        q = '''INSERT INTO  tasks
                (channel_id, added_on)
                VALUES
                (%s, NOW() );'''
        cursor.executemany(q, data2)
    except Exception as error:
        print(error)
        # LOG
        return False
        # sys.exit("Error:Failed writing new tasks to db")
    db.commit()
    return True


if __name__ == '__main__':
    q = Queue('write_tasks', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='write_tasks')
        worker.work()
