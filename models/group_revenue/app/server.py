from flask import Flask
from models.group_revenue import GroupRevenue

app = Flask(__name__)
app.logger.setLevel("DEBUG")

model = GroupRevenue()

from models.group_revenue.app import server_routes

app.logger.info("Group Revenue service startup")