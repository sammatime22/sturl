# This will talk with the remainder of the application
# sammatime22, 2022
import json
import uuid
import time


class Controller:

    ITERATOR = 1
    
    MAX_ITERATIONS = 60 # default is 60x
    
    SLEEP = 1 # default is 1s

    acquired_messages = []

    stomp_conn = None

    mariadb_conn = None

    def __init__(self, max_iterations, stomp_conn, mariadb_conn):
        '''
        Initializes the Controller.

        Parameters:
        ----------
        max_iterations : int
            The maximum iterations to be used when awaiting a message.
        stomp_conn : stomp.Connection
            The connection to STOMP to be used by the Controller.
        mariadb_conn : mariadb.connect
            The connection to be made to the MariaDB database.
        '''
        self.MAX_ITERATIONS = max_iterations
        self.stomp_conn = stomp_conn
        self.mariadb_conn = mariadb_conn


    def on_message(self, message):
        self.acquired_messages.append(json.loads(str(message.body)))
        print("collected a message: " + str(message.body))


    def on_error(self, message):
        print("Error was seen: " + str(message.body))


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
            uuid_to_use = uuid.uuid4()
            # place the job on the queue
            self.mariadb_conn.execute(\
                "INSERT INTO JOB_QUEUE (uuid, job_type, url_of_interest) " +\
                "VALUES ('%s', 'STORE', '%s');" % (uuid_to_use, url_of_interest)\
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
        try:
            # place job on queue
            self.mariadb_conn.execute(\
                "INSERT INTO JOB_QUEUE (uuid, job_type) " +\
                "VALUES ('%s', 'RETRIEVE');" % (uuid_to_query)\
            )
            itr = 0
            while itr < self.MAX_ITERATIONS:
                # listen for message
                for message in self.acquired_messages:
                    if message["uuid"] == uuid_to_query:
                        return message, True
                # return after x iterations
                itr += self.ITERATOR
                time.sleep(self.SLEEP)
        except Exception as e:
            print("Error seen: " + str(e))
        return None, False
