from flask import Flask
from models.revenue_spark import RevenueSpark

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = RevenueSpark()

from models.revenue_spark.app import server_routes

app.logger.info("Revenue Spark server service startup")