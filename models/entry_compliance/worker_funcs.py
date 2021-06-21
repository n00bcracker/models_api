from models.entry_compliance import EntryCompliance
from utils import resources

import requests

def fit_task():
    model = EntryCompliance()
    res = model.fit()
    if res[resources.RESPONSE_STATUS_FIELD] == 'Ok':
        requests.get('http://similar_clients/reboot')
    return

def predict_all_task():
    model = EntryCompliance()
    return model.predict()
