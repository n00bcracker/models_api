from flask import Flask
from models.revenue import Revenue

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = Revenue()

from models.revenue.app import server_routes

app.logger.info("Revenue service startup")