from flask import Flask
from models.advisor import AdvisorStat

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = AdvisorStat()

from models.advisor.app import server_routes
app.logger.info("Advisor Stat service startup")