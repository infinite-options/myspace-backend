from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from data_pm import connect


class StatusUpdate(Resource):
    def put(self):
        payload = request.get_json()
        response = {}
        if payload.get('maintenance_quote_uid'):
            if payload.get('quote_status') == 'REQUESTED':
                with connect() as db:
                    maintenance_quote = db.select('space_prod.maintenanceQuotes', {'maintenance_quote_uid': payload.get('maintenance_quote_uid')})
                    maintenance_request_uid = maintenance_quote.get('result')[0]['quote_maintenance_request_id']
                    response = db.update('space_prod.maintenanceRequests',
                                         {'maintenance_request_uid': maintenance_request_uid},
                                         {'maintenance_request_status': 'PROCESSING'})

            elif payload.get('quote_status') == 'SCHEDULED':
                with connect() as db:
                    maintenance_quote = db.select('space_prod.maintenanceQuotes', {'maintenance_quote_uid': payload.get('maintenance_quote_uid')})
                    maintenance_request_uid = maintenance_quote.get('result')[0]['quote_maintenance_request_id']
                    response = db.update('space_prod.maintenanceRequests',
                                         {'maintenance_request_uid': maintenance_request_uid},
                                         {'maintenance_request_status': 'SCHEDULED'})
            elif payload.get('quote_status') == 'FINISHED':
                with connect() as db:
                    maintenance_quote = db.select('space_prod.maintenanceQuotes', {'maintenance_quote_uid': payload.get('maintenance_quote_uid')})
                    maintenance_request_uid = maintenance_quote.get('result')[0]['quote_maintenance_request_id']
                    response = db.update('space_prod.maintenanceRequests',
                                         {'maintenance_request_uid': maintenance_request_uid},
                                         {'maintenance_request_status': 'COMPLETED'})
            with connect() as db:
                response_2 = db.update('space_prod.maintenanceQuotes',
                                       {'maintenance_quote_uid': payload.get('maintenance_quote_uid')},
                                       {'quote_status': payload.get('quote_status')})

        elif payload.get('maintenance_request_uid'):
            if payload.get('maintenance_request_status') == 'CANCELLED':
                with connect() as db:
                    maintenance_request_uid = payload.get('maintenance_request_uid')
                    response = db.update('space_prod.maintenanceQuotes',
                                         {'quote_maintenance_request_id': maintenance_request_uid},
                                         {'quote_status': 'WITHDRAWN'})
            with connect() as db:
                response_2 = db.update('space_prod.maintenanceRequests',
                                       {'maintenance_request_uid': payload.get('maintenance_request_uid')},
                                       {'maintenance_request_status': payload.get('maintenance_request_status')})

        else:
            raise BadRequest("Request failed, no quote in payload.")
        return response