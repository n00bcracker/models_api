from models.clients_complience.app.server import app, model, queue
from models.clients_complience.worker_funcs import fit_task, predict_all_task
from utils import resources
import traceback
import os, signal
from flask import request, jsonify
import requests

@app.route('/', methods=['POST'])
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
        resp_data.update(model.get_predict(inn, kpp=kpp))
        response = jsonify(resp_data)
        response.status_code = 200
        return response

@app.route('/fit', methods=['GET', 'POST'])
def model_fit():
    resp_data = {
                 resources.RESPONSE_STATUS_FIELD: None,
                 resources.RESPONSE_ERROR_FIELD: None
                 }
    if request.method == 'GET':
        try:
            job = queue.enqueue(fit_task, result_ttl=600, job_timeout='90m')
        except Exception:
            error = traceback.format_exc()
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
            resp_data[resources.RESPONSE_ERROR_FIELD] = error
            app.logger.error(error)
            response = jsonify(resp_data)
            response.status_code = 200
            return response
        else:
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Ok'
            resp_data['job_id'] = job.id
            response = jsonify(resp_data)
            response.status_code = 200
            return response
    elif request.method == 'POST':
        try:
            req_json = request.get_json()
            resp_data['job_id'] = req_json['job_id']
        except Exception:
            errors = traceback.format_exc()
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
            resp_data[resources.RESPONSE_ERROR_FIELD] = errors
            app.logger.error(errors)
            response = jsonify(resp_data)
            response.status_code = 200
            return response
        else:
            job = queue.fetch_job(resp_data['job_id'])
            job_status = job.get_status()
            if job_status in ['queued', 'started', 'deferred']:
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Executing'
            elif job_status == 'failed':
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                resp_data[resources.RESPONSE_ERROR_FIELD] = job.exc_info
            elif job_status == 'finished':
                resp_data.update(job.result)
                if resp_data[resources.RESPONSE_STATUS_FIELD] == 'Ok':
                    requests.get('http://entry_compliance:3310/reboot')

            response = jsonify(resp_data)
            response.status_code = 200
            return response

@app.route('/predict', methods=['GET', 'POST'])
def model_predict():
    resp_data = {
        resources.RESPONSE_STATUS_FIELD: None,
        resources.RESPONSE_ERROR_FIELD: None
    }

    if request.method == 'GET':
        try:
            job = queue.enqueue(predict_all_task, result_ttl=600, job_timeout='45m')
        except Exception:
            error = traceback.format_exc()
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
            resp_data[resources.RESPONSE_ERROR_FIELD] = error
            app.logger.error(error)
            response = jsonify(resp_data)
            response.status_code = 200
            return response
        else:
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Ok'
            resp_data['job_id'] = job.id
            response = jsonify(resp_data)
            response.status_code = 200
            return response
    elif request.method == 'POST':
        try:
            req_json = request.get_json()
            resp_data['job_id'] = req_json['job_id']
        except Exception:
            errors = traceback.format_exc()
            resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
            resp_data[resources.RESPONSE_ERROR_FIELD] = errors
            app.logger.error(errors)
            response = jsonify(resp_data)
            response.status_code = 200
            return response
        else:
            job = queue.fetch_job(resp_data['job_id'])
            job_status = job.get_status()
            if job_status in ['queued', 'started', 'deferred']:
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Executing'
            elif job_status == 'failed':
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                resp_data[resources.RESPONSE_ERROR_FIELD] = job.exc_info
            elif job_status == 'finished':
                resp_data.update(job.result)

            response = jsonify(resp_data)
            response.status_code = 200
            return response

@app.route('/reboot', methods=['GET'])
def reboot_gunicorn():
    resp_data = {
        resources.RESPONSE_STATUS_FIELD: None,
        resources.RESPONSE_ERROR_FIELD: None
    }

    try:
        os.kill(os.getppid(), signal.SIGHUP)
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