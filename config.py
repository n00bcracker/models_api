import os

PROJECT_BASEDIR = os.path.dirname(os.path.abspath(__file__))
FLASK_TEMPLATE_PATH = os.path.join(PROJECT_BASEDIR, "data", "templates")

ORACLE_USERNAME = os.getenv("ORACLE_USER") or None
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD") or None
ORACLE_TNS = os.getenv("ORACLE_TNS") or None


SPARK_REVENUE_TABLE = 'ev_prognoz_viruchka_all'