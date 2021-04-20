from models.advisor.app.server import app, model
from utils import resources
import traceback
from flask import request, jsonify
import os, signal
#from flask import Response


@app.route('/financial-advisor', methods=['POST', 'GET'])
def get_data_from_advisor():
    resp_data = {#resources.REQUEST_ID_FIELD: None,
                 #resources.RESPONSE_STATUS_FIELD: None,
                 #resources.RESPONSE_ERROR_FIELD: None
                 }            

    try:
                    req_json = request.get_json()  
                    inn = req_json.get('inn', None)
                    kpp = req_json.get('kpp', None)
                    if kpp == '': kpp = None
                    fio = req_json.get('fullName', None)
                 
    except Exception:
                    errors = traceback.format_exc()
                    resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    resp_data[resources.RESPONSE_ERROR_FIELD] = errors
                    app.logger.error(errors)
                    response = jsonify(resp_data)
                    response.status_code = 200
                    return response
    else:
                    resp_data.update(model.get_block_predict(inn, kpp, fio))
                    response = jsonify(resp_data)
                    response.status_code = 200
                    return response

@app.route('/financial-advisor/congratulations', methods=['POST'])
def update_data_in_events():
    resp_data = {#resources.REQUEST_ID_FIELD: None,
                 #resources.RESPONSE_STATUS_FIELD: None,
                 #resources.RESPONSE_ERROR_FIELD: None
                 }
    try:
                req_json = request.get_json()          
                inn = req_json.get('inn', None)
                kpp = req_json.get('kpp', None)
                if kpp == '': kpp = None
                congrat = req_json.get('congratulationCode', None)
                date = req_json.get('celebrationYear', None)

    except Exception:
                errors = traceback.format_exc()
                resp_data[resources.RESPONSE_STATUS_FIELD] = 'Error'
                resp_data[resources.RESPONSE_ERROR_FIELD] = errors
                app.logger.error(errors)
                response = jsonify(resp_data)
                response.status_code = 200
                return response
    else:
                resp_data.update(model.post_update(inn, kpp, congrat, date))
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