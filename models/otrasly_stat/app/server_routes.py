from models.otrasly_stat.app.server import app, model
from utils import resources
import traceback
from flask import request, jsonify
import os, signal


@app.route('/', methods=['POST'])
def get_model_result():
    resp_data = {resources.REQUEST_ID_FIELD: None,
                 resources.RESPONSE_STATUS_FIELD: None,
                 resources.RESPONSE_ERROR_FIELD: None
                 }
    try:
        req_json = request.get_json()
        resp_data[resources.REQUEST_ID_FIELD] = req_json[resources.REQUEST_ID_FIELD]
        okved = req_json['okved']
        resp_data['okved'] = okved

    except Exception:
        errors = traceback.format_exc()
        resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
        resp_data[resources.RESPONSE_ERROR_FIELD] = errors
        app.logger.error(errors)
        response = jsonify(resp_data)
        response.status_code = 200
        return response
    else:
        resp_data.update(model.get_predict(okved))
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