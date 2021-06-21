from flask import Flask
from rq import Queue
from redis import Redis
import schedule
import time, datetime
from models.similar_clients import SimilarClients

app = Flask(__name__)
app.logger.setLevel("DEBUG")

redis_conn = Redis(host='redis')
queue = Queue(connection=redis_conn)
model = SimilarClients()

from models.similar_clients.app import server_routes

app.logger.info("Similar clients service startup")

# def monthly_fitting():
#     if datetime.date.today().day == 14:
#         model.fit()
#
# schedule.every().day.at('08:00:00').do(monthly_fitting)

# while True:
#     schedule.run_pending()
#     time.sleep(1)