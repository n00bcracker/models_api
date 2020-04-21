from models.similar_companies.app.master import app, model
from utils import resources
import traceback
from flask import request, jsonify
import requests

@app.route('/fit', methods=['GET'])
def start_model_fit():
    resp_data = {
                 resources.RESPONSE_STATUS_FIELD: None,
                 resources.RESPONSE_ERROR_FIELD: None
                 }

    try:
        # model.fit()
        requests.get('http://similar_companies_server:3344/reboot')
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