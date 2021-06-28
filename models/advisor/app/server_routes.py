from models.advisor.app.server import app, model
from utils import resources
import traceback
from flask import request, jsonify
import os, signal
#from flask import Response


@app.route('/', methods=['GET'])
def get_advisor_results():
    resp_data = dict()

    try:
        args = request.args
        organization_id = args['organizationId']
        person_id = args['personId']
        module_name = args.get('unit')
                 
    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют необходимые аргументы запроса'
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 400
        return response
    else:
        resp_data.update(model.get_predict(organization_id, person_id, module_name))
        response = jsonify(resp_data)
        if resp_data.get(resources.RESPONSE_ERROR_FIELD) is None:
            response.status_code = 200
        else:
            response.status_code = 500
        return response

# @app.route('/show', methods=['GET'])
# def check_actual_suggestions():
#     resp_data = dict()
#
#     try:
#         args = request.args
#         organization_id = args['organizationId']
#         person_id = args['personId']
#     except Exception:
#         errors = traceback.format_exc()
#         resp_data[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют необходимые аргументы запроса'
#         app.logger.error(errors)
#         response = jsonify(resp_data)
#         response.status_code = 400
#         return response
#     else:
#         resp_data.update(model.check_actual_suggestions(organization_id, person_id))
#         response = jsonify(resp_data)
#         if resp_data.get(resources.RESPONSE_ERROR_FIELD) is None:
#             response.status_code = 200
#         else:
#             response.status_code = 500
#         return response

@app.route('/congratulations', methods=['POST'])
def update_data_in_events():
    resp_data = dict()
    try:
        req_json = request.get_json()
        organizationId = req_json['organizationId']
        person_id = req_json['personId']
        congr_code = req_json['congratulationCode']
        celebr_year = req_json['celebrationYear']

    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют необходимые аргументы запроса'
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 400
        return response
    else:
        resp_data.update(model.process_user_feedback(organizationId, person_id, congr_code, celebr_year))
        response = jsonify(resp_data)
        if resp_data.get(resources.RESPONSE_ERROR_FIELD) is None:
            response.status_code = 200
        else:
            response.status_code = 500
        return response

@app.route('/invite', methods=['GET'])
def show_counterparties():
    resp_data = dict()

    try:
        args = request.args
        organization_id = args['organizationId']
        person_id = args['personId']

    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют необходимые аргументы запроса'
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 400
        return response
    else:
        resp_data.update(model.get_suitable_counerparties(organization_id))
        response = jsonify(resp_data)
        if resp_data.get(resources.RESPONSE_ERROR_FIELD) is None:
            response.status_code = 200
        else:
            response.status_code = 500
        return response