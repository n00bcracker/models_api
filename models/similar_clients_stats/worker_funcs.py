from utils import resources

import requests

def aggr_task(request_json):
        with requests.session() as session:
            resp = session.post('http://similar_clients_stats:3321/', json=request_json)
            if resp is not None and resp.status_code == requests.codes.ok:
                resp_data = resp.json()
            else:
                resp_data = dict()
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                resp_data[resources.RESPONSE_ERROR_FIELD] = 'Сервис расчета показателей по 9 соседям не отвечает'

            session.post('http://open_academy.ru/', json=resp_data)
        return 0
