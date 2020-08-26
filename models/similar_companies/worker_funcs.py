from models.similar_companies import SimilarCompanies
from utils import resources

import requests

def fit_task():
    model = SimilarCompanies()
    res = model.fit()
    if res[resources.RESPONSE_STATUS_FIELD] == 'Ok':
        requests.get('http://similar_companies:3320/reboot')
    return res
