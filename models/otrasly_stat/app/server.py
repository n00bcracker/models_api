from flask import Flask
from rq import Queue
from redis import Redis
from models.otrasly_stat import OtrasliStat

app = Flask(__name__)
app.logger.setLevel("DEBUG")

redis_conn = Redis(host='redis')
queue = Queue(connection=redis_conn)
model = OtrasliStat()


app.logger.info("Otrasly Stat service startup")