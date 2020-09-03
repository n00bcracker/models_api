from flask import Flask
import schedule
import time, datetime
from models.similar_clients_stats import SimilarClientsStats

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = SimilarClientsStats()

from models.similar_clients_stats.app import server_routes

app.logger.info("Similar companies statistic server service startup")
