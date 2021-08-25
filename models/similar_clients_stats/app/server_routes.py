from models.similar_clients_stats.app.server import app, model, queue
from models.similar_clients_stats.worker_funcs import aggr_task
from utils import resources
import traceback
from flask import request, jsonify
import requests


def get_similar_clients(request_id, inn, kpp=None):
    req_data = {
        'request_id': request_id,
        'inn': inn,
        'kpp': kpp,
    }
    with requests.session() as session:
        resp = session.post('http://similar_clients/', json=req_data)
        return resp


@app.route('/', methods=['POST'])
def neighbours_stats():
    resp_data = {resources.REQUEST_ID_FIELD: None,
                 resources.RESPONSE_STATUS_FIELD: None,
                 resources.RESPONSE_ERROR_FIELD: None
                 }
    try:
        req_json = request.get_json()
        req_id = req_json[resources.REQUEST_ID_FIELD]
        resp_data[resources.REQUEST_ID_FIELD] = req_id
        queue.enqueue(aggr_task, req_json, result_ttl=60, job_timeout='5m')
    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
        resp_data[resources.RESPONSE_ERROR_FIELD] = errors
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 200
        return response
    else:
        resp_data[resources.RESPONSE_STATUS_FIELD] = 'Ok'
        response = jsonify(resp_data)
        response.status_code = 200
        return response


@app.route('/neighbours_stats', methods=['POST'])
def get_model_result():
    resp_data = {resources.REQUEST_ID_FIELD: None,
                 resources.RESPONSE_STATUS_FIELD: None,
                 resources.RESPONSE_ERROR_FIELD: None
                 }
    try:
        req_json = request.get_json()
        resp_data[resources.REQUEST_ID_FIELD] = req_json[resources.REQUEST_ID_FIELD]
        inn = req_json['inn']
        kpp = req_json.get('kpp', None)
        resp_data['inn'] = inn
        resp_data['kpp'] = kpp
    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
        resp_data[resources.RESPONSE_ERROR_FIELD] = errors
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 200
        return response
    else:
        sim_cls_resp = get_similar_clients(resp_data[resources.REQUEST_ID_FIELD], inn, kpp)
        if sim_cls_resp is not None and sim_cls_resp.status_code == requests.codes.ok:
            sim_cls_data = sim_cls_resp.json()
            if sim_cls_data[resources.REQUEST_ID_FIELD] == resp_data[resources.REQUEST_ID_FIELD]:
                if sim_cls_data[resources.RESPONSE_STATUS_FIELD] == 'Ok':
                    resp_data['inn'] = sim_cls_data['inn']
                    resp_data['kpp'] = sim_cls_data['kpp']
                    sim_cls = sim_cls_data['similar_clients']
                    resp_data.update(model.similar_clients_aggr(sim_cls))
                else:
                    resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    resp_data[resources.RESPONSE_ERROR_FIELD] = sim_cls_data[resources.RESPONSE_ERROR_FIELD]
            else:
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                resp_data[resources.RESPONSE_ERROR_FIELD] = 'Идентификаторы запроса и ответа не совпадают'
        else:
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
            resp_data[resources.RESPONSE_ERROR_FIELD] = 'Сервис расчета 9 соседей не отвечает'
        response = jsonify(resp_data)
        response.status_code = 200
        return response

