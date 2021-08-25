from models.similar_clients import SimilarClients
from utils import resources

import requests

def fit_task():
    model = SimilarClients()
    res = model.fit()
    if res[resources.RESPONSE_STATUS_FIELD] == 'Ok':
        requests.get('http://similar_clients/reboot')
    return res
