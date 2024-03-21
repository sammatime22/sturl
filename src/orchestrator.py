# This portion takes and prescribes jobs to different workers.
# sammatime22, 2024
import datetime
import json
import stomp
import time

class Orchestrator(stomp.ConnectionListener):
    '''
    The main orchestrator of the STURL backend, tasking different workers with jobs.
    '''

    # default is 1s
    SLEEP = 1

    # Connection to MariaDB
    mariadb_conn_cursor = None

    # Checks to see if the Orchestrator should continue running
    continue_running = True

    # finds the most recent job to task
    find_most_recent_untasked_job = "SELECT uuid, url_of_interest FROM JOB_QUEUE WHERE tasked_resource = NULL ORDER BY insert_time LIMIT 1;"

    # finds an untasked data worker
    find_untasked_data_worker = "SELECT resource_id FROM DATA WORKERS WHERE tasked = 0 ORDER BY free_since;"

    # updates a worker's status
    update_data_worker_status = "UPDATE DATA_WORKERS SET tasked=%d, current_task=%s, updated_at=NOW() WHERE resource_id=%d;"

    def __init__(self,  mariadb_conn_cursor):
        '''
        Initializes the Orchestrator.

        Parameters:
        ----------
        '''
        self.mariadb_conn_cursor = mariadb_conn_cursor


    def on_message(self, headers, message):
        '''
        Pulls in messages for the Orchestrator
        '''
        print("received a message " + str(message) + " with header " + str(headers))
        response = json.loads(message)
        worker_id = headers["worker_id"]
        successful = response["successful"]
        completed_time = str(datetime.datetime.now()).split(".")[0]
        self.mariadb_conn_cursor.execute(self.update_data_worker_status % (0, "NULL", worker_id))

    def on_error(self, headers, message):
        '''
        Handles message errors for the Orchestrator
        '''
        print("could not interperet message " + str(message) + " with header " + str(headers))


    def check_and_send_job(self):
        '''
        Sends a store or retrieve job, depending on what is available in the DB.
        '''
        try:
            self.mariadb_conn_cursor.execute(self.find_most_recent_untasked_job)
            for (uuid, url_of_interest) in self.mariadb_conn_cursor:
                self.mariadb_conn_cursor.execute(self.find_untasked_data_worker)
                for (resource_id) in self.mariadb_conn_cursor:
                    stomp.send(body="{\"uuid\":\"" + str(uuid) + "\", \"urlOfInterest\":\"" + str(url_of_interest) + "\"}", destination="/data-worker-" + resource_id)
                    self.mariadb_conn_cursor.execute(self.update_data_worker_status % (str(uuid), resource_id))
        except Exception as e:
            print("Error seen: " + str(e))


    def main_loop(self):
        '''
        This method runs continuously, continuing a modular flow within STURL.
        '''
        print("Orchestrator booted and running...")

        while self.continue_running:
            try:
                # Check if we have a new job to schedule
                self.check_and_send_job()
                time.sleep(self.SLEEP)
            except Exception as e:
                print("Error seen: " + str(e))
