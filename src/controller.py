# This will talk with the remainder of the application
# sammatime22, 2024
import json
import uuid


class Controller():
    '''
    This class takes in well formatted commands from the interface, and utilizes
    the remainder of the resources in the backend via various means.
    '''

    RESULTS = "results"

    # A 1s iterator, to not be overwritten
    ITERATOR = 1
    
    # default is 60x, or 60s
    MAX_ITERATIONS = 60 
    
    # default is 1s
    SLEEP = 1 

    # Connection to MariaDB
    mariadb_conn = None

    # Connection to MongoDB
    mongo_db_conn = None

    def __init__(self, mariadb_conn, mongo_db_conn):
        '''
        Initializes the Controller.

        Parameters:
        ----------
        max_iterations : int
            The maximum iterations to be used when awaiting a message.
        mariadb_conn : mariadb.connect
            The connection to be made to the MariaDB database.
        mongo_db_conn : mongo_db_conn.connect
            The connection to be made to the MongoDB database.
        '''
        self.mariadb_conn = mariadb_conn
        self.mongo_db_conn = mongo_db_conn


    def request_data(self, url_of_interest):
        '''
        This is the method used by the controller to request data for a particular URL.

        Parameters:
        ----------
        url_of_interest : string
            The url to request to be queried.
        '''
        try:
            # create an ID for the job
            uuid_to_use = uuid.uuid1()
            # place the job on the queue
            self.mariadb_conn.execute(\
                "INSERT INTO JOB_QUEUE (uuid, url_of_interest) " +\
                "VALUES ('%s', '%s');" % (uuid_to_use, url_of_interest)\
            )
            return uuid_to_use, True
        except Exception as e:
            print("Error seen: " + str(e))
        return None, False


    def collect_report(self, uuid_to_query):
        '''
        This method collects a report based on the uuid provided.

        Parameters:
        ----------
        uuid_to_query : string
            The uuid associated to the data of interest.
        '''
        results = mongo_db_conn[RESULTS][uuid_to_query]
        return results, results is not None
