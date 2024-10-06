from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class DB(object):
    _instance = None 

    def __new__(cls, uri):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DB, cls).__new__(cls)
            cls.instance._initialized = False
            cls.instance.setup_db(uri)  # Automatically set up the database upon instance creation
        return cls.instance
    
    def setup_db(self, uri):
        if not self._initialized:
            print("Setting up db")
            client = MongoClient(uri, server_api=ServerApi('1'))
            self.db = client.get_database()
            self._initialized = True

    def get_db(self):
        return self.db
