from models.similar_companies import SimilarCompanies
from flask import Flask
import schedule
import time, datetime
import pandas as pd
import pickle

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = SimilarCompanies(app)

from models.similar_companies.app import master_routes

app.logger.info("Similar companies master service startup")

# def monthly_fitting():
#     if datetime.date.today().day == 14:
#         model.fit()
#
# schedule.every().day.at('08:00:00').do(monthly_fitting)

# while True:
#     schedule.run_pending()
#     time.sleep(1)