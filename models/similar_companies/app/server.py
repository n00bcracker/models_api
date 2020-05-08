from flask import Flask
from models.similar_companies import SimilarCompanies

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = SimilarCompanies()

from models.similar_companies.app import server_routes

app.logger.info("Similar companies server service startup")