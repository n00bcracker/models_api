from flask import Flask
from rq import Queue
from redis import Redis
from models.entry_compliance import EntryCompliance

app = Flask(__name__)
app.logger.setLevel("DEBUG")

redis_conn = Redis(host='redis')
queue = Queue(connection=redis_conn)
model = EntryCompliance()

from models.entry_compliance.app import server_routes

app.logger.info("Entry Compliance server service startup")
