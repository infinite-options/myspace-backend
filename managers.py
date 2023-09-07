from flask import request
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity

from data_pm import connect, uploadImage, s3

class SearchManager(Resource):

    # def get(self):
    #     response = {}
    #     business_location = request.args.get('business_location')
    #     business_name = request.args.get('business_name')

    #     with connect() as db:
    #         searchQuery = f"""
    #         SELECT *
    #         FROM space.businessProfileInfo
    #         JOIN JSON_TABLE(
    #         space.businessProfileInfo.business_locations,
    #         '$[*]'
    #         COLUMNS (
    #             distance VARCHAR(255) PATH '$.distance',
    #             location VARCHAR(255) PATH '$.location'
    #         )
    #         ) AS json_data ON json_data.location = '{business_location}'
    #         WHERE space.businessProfileInfo.business_name = '{business_name}';
    #         """
    #         # print(searchQuery)
    #         response = db.execute(searchQuery)
    #     return response

    def get(self):
        response = {}
        # filters = ['business_uid', 'business_type',
        #            'business_name', 'business_locations']
        where = {}
        # for filter in filters:
        #     filterValue = request.args.get(filter)
        #     if filterValue is not None:
        #         where[filter] = filterValue
        where['business_type'] = 'MANAGEMENT'
        # print(where)
        with connect() as db:
            response = db.select('b_details', where)
        return response