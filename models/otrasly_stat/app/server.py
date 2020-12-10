from flask import Flask
from models.otrasly_stat import OtrasliStat

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = OtrasliStat()

from models.otrasly_stat.app import server_routes
app.logger.info("Otrasly Stat service startup")