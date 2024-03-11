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

    def __init__(self, worker_number, mongo_db_conn):
        '''
        Initializes the Data Worker
        -----------------------------
        worker_number : number
          an identifier for the Data Worker
        '''
        self.worker_number = worker_number
        self.mongo_db_conn = mongo_db_conn


    def on_message(self, headers, message):
        '''
        Pulls in messages for the Data Worker
        '''
        print("received a message " + str(message) + " with header " + str(headers))
        data = pull_from_web(json.loads(str(message)))
        if data is not None:
            report = build_report(data)
            if report is not None:
                if insert_report(report):
                    stomp.send(body="sucessfully completed job " + str(data["uuid"]))
                else:
                    stomp.send(body="could not insert report for job " + str(data["uuid"]))
            else:
                stomp.send(body="unable to interperet data for job " + str(data["uuid"]))
        else:
            stomp.send(body="unable to interperet data for job " + str(data["uuid"]))
        insert_report(report)


    def on_error(self, headers, message):
        '''
        Handles message errors for the Data Worker
        '''
        print("could not interperet message " + str(message) + " with header " + str(headers))


    def pull_from_web(url):
        '''
        Pulls data from the web
        '''
        try:
            return requests.get(url)
        except Exception as e:
            print(e)
            return None


    def build_report(data):
        '''
        Builds the report to send back based on the data recieved
        '''
        try:
            report = {}
            report[self.FINAL_DESTINATION] = data.url
            report[self.HAD_REDIRECT] = data.history
            report[self.STATUS_CODE] = data.status_code
            report[self.COOKIES] = data.cookies
            report[self.HEADER_CONTENT] = data.headers
            report[self.LINKS_ON_SITE] = data.linksOnSite
            report[self.POTENTIAL_JAVASCRIPT_FUNCTIONS] = extract_functions(data.text)
            return report
        except Exception as e:
            print(e)
            return None


    def extract_functions(page_content):
        '''
        Attempts to extract functions from the webpage content
        '''
        try:
            potential_functions = []
            function_indeces = [m.start() for m in re.finditer(self.FUNCTION, page_content.text)]
            for index_of_interest in function_indeces:
                start_pos = index_of_interest
                end_pos = index_of_interest
                current_char = page_content.text[start_pos]
                while current_char != OPEN_BRACKET:
                    start_pos = start_pos + 1
                    end_pos = end_pos + 1
                    current_char = page_content.text[start_pos]
                bracket_count = 1
                while bracket_count != 0:
                    end_pos = end_pos + 1
                    current_char = page_content.text[end_pos]
                    if current_char == OPEN_BRACKET:
                        bracket_count = bracket_count + 1
                    elif current_char == CLOSE_BRACKET:
                        bracket_count = bracket_count - 1
                potential_functions.append(page_content.text[start_pos:end_pos])
            return potential_functions
        except Exception as e:
            print(e)
            return None


    def insert_report(uuid, data):
        '''
        Inserts the report into MongoDB
        '''
        try:
            my_collection = mongo_db_conn[uuid]
            my_collection.insert_one(data)
            return True
        except Exception as e:
            print(e)
            return False
