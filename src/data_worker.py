# A worker that collects data from the internet, based on a provided url, and inserts it into MongoDB.
# sammatime22, 2024
import json
import re
import requests
import stomp

class DataWorker(stomp.ConnectionListener):
    '''
    The worker thread that pulls reuests from the internet, and puts it in MongoDB.
    '''

    FINAL_DESTINATION = "finalDestination"

    HAD_REDIRECT = "hadRedirect"

    STATUS_CODE = "statusCode"

    COOKIES = "cookies"

    HEADER_CONTENT = "headerContent"

    LINKS_ON_SITE = "linksOnSite"

    POTENTIAL_JAVASCRIPT_FUNCTIONS = "potentialJavascriptFunctions"

    FUNCTION = "function()"

    OPEN_BRACKET = "{"

    CLOSE_BRACKET = "}"

    worker_number = None

    mongo_db_conn = None

    stomp_connection = None

    def __init__(self, worker_number, mongo_db_conn):
        '''
        Initializes the Data Worker
        -----------------------------
        worker_number : number
          an identifier for the Data Worker
        '''
        self.worker_number = worker_number
        self.mongo_db_conn = mongo_db_conn


    def on_message(self, message):
        '''
        Pulls in messages for the Data Worker
        '''
        message_body = json.loads(str(message.body))
        if (message_body.get("dataWorker") is None) or (message_body["dataWorker"] != self.worker_number):
            return
        print("got a message for " + str(message_body["dataWorker"]))
        data = self.pull_from_web(message_body["urlOfInterest"])

        if data is not None:
            report = self.build_report(data)
            if report is not None:
                if self.insert_report(message_body["uuid"], report):
                    self.stomp_connection.send("/data-worker", "{\"uuid\":\"" + str(message_body["uuid"]) + "\", \"successful\": true, \"dataWorkerResponsible\": " + str(self.worker_number) + "}")
                else:
                    self.stomp_connection.send("/data-worker", "{\"uuid\":\"" + str(message_body["uuid"]) + "\", \"successful\": false, \"dataWorkerResponsible\": " + str(self.worker_number) + "}")
            else:
                self.stomp_connection.send("/data-worker", "{\"uuid\":\"" + str(message_body["uuid"]) + "\", \"successful\": false, \"dataWorkerResponsible\": " + str(self.worker_number) + "}")
        else:
            self.stomp_connection.send("/data-worker", "{\"uuid\":\"" + str(message_body["uuid"]) + "\", \"successful\": false, \"dataWorkerResponsible\": " + str(self.worker_number) + "}")


    def on_error(self, message):
        '''
        Handles message errors for the Data Worker
        '''
        print("could not interperet message " + str(message))


    def set_stomp_connection(self, stomp_connection):
        self.stomp_connection = stomp_connection


    def pull_from_web(self, url):
        '''
        Pulls data from the web
        '''
        try:
            return requests.get(url)
        except Exception as e:
            print(e)
            return None


    def build_report(self, data):
        '''
        Builds the report to send back based on the data recieved
        '''
        try:
            report = {}
            report[self.FINAL_DESTINATION] = str(data.url)
            report[self.HAD_REDIRECT] = str(data.history)
            report[self.STATUS_CODE] = str(data.status_code)
            report[self.COOKIES] = str(data.cookies)
            report[self.HEADER_CONTENT] = str(data.headers)
            report[self.LINKS_ON_SITE] = str(data.links)
            report[self.POTENTIAL_JAVASCRIPT_FUNCTIONS] = self.extract_functions(data.text)
            return report
        except Exception as e:
            print(e)
            return None


    def extract_functions(self, page_content):
        '''
        Attempts to extract functions from the webpage content
        '''
        try:
            potential_functions = []
            function_indeces = [m.start() for m in re.finditer(self.FUNCTION, page_content)]
            for index_of_interest in function_indeces:
                start_pos = index_of_interest
                end_pos = index_of_interest
                current_char = page_content[start_pos]
                while current_char != self.OPEN_BRACKET:
                    start_pos = start_pos + 1
                    end_pos = end_pos + 1
                    current_char = page_content[start_pos]
                bracket_count = 1
                while bracket_count != 0:
                    end_pos = end_pos + 1
                    current_char = page_content[end_pos]
                    if current_char == self.OPEN_BRACKET:
                        bracket_count = bracket_count + 1
                    elif current_char == self.CLOSE_BRACKET:
                        bracket_count = bracket_count - 1
                potential_functions.append(page_content[start_pos:end_pos])
            return potential_functions
        except Exception as e:
            print(e)
            return None


    def insert_report(self, uuid, data):
        '''
        Inserts the report into MongoDB
        '''
        try:
            sturl_db = self.mongo_db_conn.sturl_db
            my_collection = sturl_db[uuid]
            my_collection.insert_one(data)
            return True
        except Exception as e:
            print(e)
            return False
