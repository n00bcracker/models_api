from models.similar_companies.app.master import app, queue
from models.similar_companies.worker_funcs import fit_task
from utils import resources
import traceback
from flask import request, jsonify
import requests

@app.route('/fit', methods=['GET', 'POST'])
def model_fit():
    resp_data = {
                 resources.RESPONSE_STATUS_FIELD: None,
                 resources.RESPONSE_ERROR_FIELD: None
                 }

    if request.method == 'GET':
        try:
            job = queue.enqueue(fit_task, result_ttl=600)
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
                requests.get('http://similar_companies_server:3344/reboot')

            response = jsonify(resp_data)
            response.status_code = 200
            return response