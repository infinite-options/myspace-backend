# ----DNU-----

from flask import request, jsonify
from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity
from functools import wraps

def check_jwt_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            # Verifies the token in the request
            verify_jwt_in_request()

             # Extract the user identity from the token (e.g., user_id)
            current_user_id = get_jwt_identity()
            
            # Assuming you have the expected user_id available, e.g., from the current session or route parameters
            expected_user_id = kwargs.get('user_id')  # Or from some other source like the URL parameter
            
            # Compare the user_id in the token with the expected user_id
            if current_user_id != expected_user_id:
                return jsonify({'message': 'You do not have permission to access this resource!'}), 403
        except Exception as e:
            return jsonify({'message': 'Invalid or expired token!'}), 401

        # Proceed with the original function if token is valid
        return f(*args, **kwargs)

    return decorated
