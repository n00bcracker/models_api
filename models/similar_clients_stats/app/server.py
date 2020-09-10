from flask import Flask
from rq import Queue
from redis import Redis
from models.similar_clients_stats import SimilarClientsStats

app = Flask(__name__)
app.logger.setLevel("DEBUG")


redis_conn = Redis(host='redis')
queue = Queue(connection=redis_conn)
model = SimilarClientsStats()

from models.similar_clients_stats.app import server_routes

app.logger.info("Similar companies statistic server service startup")
