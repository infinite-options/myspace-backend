# manifiestmyspace-backend README File

# Use this in cmd to run the pytest script (test_api): python -m pytest -v -s

# ----------------------------------------------------------------------------

# LOCAL MODE INSTRUCTION

# CHANGE env FILE TO TURN ENCRYPTION ON OR OFF AS FOLLOWS:

    ENCRYPTION_ENABLED = false to turn encryption OFF
    ENCRYPTION_ENABLED = true to turn encryption ON

# ----------------------------------------------------------------------------

# ZAPPA UPDATE INSTRUCTIONS

# Settings for PRODUCTION MODE:

    RDS_DB = space_prod
    BUCKET_NAME = io-pm

# ZAPPA UPDATE USE: zappa update production

# Settings for DEV MODE:

    RDS_DB = space_dev
    BUCKET_NAME = io-myspace

# ZAPPA UPDATE USE: zappa update dev
