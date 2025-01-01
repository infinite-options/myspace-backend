from flask import request
from flask_restful import Resource

from data_pm import connect


class List(Resource):
    def get(self):
        response = {}
        where = request.args.to_dict()
        with connect() as db:
            response = db.select('space_prod.lists', where)
        return response
