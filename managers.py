from flask import request
from flask_restful import Resource
from data_pm import connect

class SearchManager(Resource):
    def get(self):
        response = {}
        args_dict = request.args.to_dict()
        field_name = 'business_name'
        with connect() as db:
            where = {'business_type': 'MANAGEMENT'}
            if field_name in args_dict:
                where[field_name] = args_dict[field_name]
            response = db.select('space_prod.businessProfileInfo', where, exact_match=False)
        return response