import os

PROJECT_BASEDIR = os.path.dirname(os.path.abspath(__file__))
METAFILES_DIR = os.path.join(PROJECT_BASEDIR, 'models', 'meta')

ORACLE_USERNAME = os.getenv("ORACLE_USER") or None
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD") or None
ORACLE_TNS = os.getenv("ORACLE_TNS") or None