from models.advisor.app.server import app, model
from utils import resources
import traceback
from flask import request, jsonify
import os, signal
#from flask import Response


@app.route('/financial-advisor', methods=['POST'])
def get_advisor_results():
    resp_data = {#resources.REQUEST_ID_FIELD: None,
                 #resources.RESPONSE_STATUS_FIELD: None,
                 #resources.RESPONSE_ERROR_FIELD: None
                 }            

    try:
        req_json = request.get_json()
        inn = req_json['inn']
        kpp = req_json.get('kpp', None)
        if kpp == '':
            kpp = None
        lpr_fullname = req_json['fullName']
                 
    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
        resp_data[resources.RESPONSE_ERROR_FIELD] = errors
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 400
        return response
    else:
        resp_data.update(model.get_predict(inn, lpr_fullname, kpp=kpp))
        response = jsonify(resp_data)
        if resp_data.get(resources.RESPONSE_ERROR_FIELD) is None:
            response.status_code = 200
        else:
            response.status_code = 500
        return response

@app.route('/financial-advisor/congratulations', methods=['POST'])
def update_data_in_events():
    resp_data = {#resources.REQUEST_ID_FIELD: None,
                 #resources.RESPONSE_STATUS_FIELD: None,
                 #resources.RESPONSE_ERROR_FIELD: None
                 }
    try:
        req_json = request.get_json()
        inn = req_json['inn']
        kpp = req_json.get('kpp', None)
        if kpp == '':
            kpp = None
        congr_code = req_json['congratulationCode']
        celebr_year = req_json['celebrationYear']

    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
        resp_data[resources.RESPONSE_ERROR_FIELD] = errors
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 400
        return response
    else:
        resp_data.update(model.process_user_feedback(inn, congr_code, celebr_year, kpp=kpp))
        response = jsonify(resp_data)
        if resp_data.get(resources.RESPONSE_ERROR_FIELD) is None:
            response.status_code = 200
        else:
            response.status_code = 500
        return response