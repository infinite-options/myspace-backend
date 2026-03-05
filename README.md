# manifiestmyspace-backend

# Use this in cmd to run the pytest script

# This will run test_api

# python -m pytest -v -s

# Change .env file to go between Production Mode and Dev Mode as follows:

# For PRODUCTION MODE:

# RDS_DB = space_prod

# BUCKET_NAME = io-pm

# For DEV MODE:

# RDS_DB = space_dev

# BUCKET_NAME = io-myspace


# For Production use zappa update production
# For Testing use zappa update dev
# That forces Encryption into the correct state
