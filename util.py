from datetime import datetime
from hashlib import sha256


def getHash(value):
    base = str(value).encode()
    return sha256(base).hexdigest()


def createSalt():
    return getHash(datetime.now())


def createHash(password, salt):
    return getHash(password+salt)