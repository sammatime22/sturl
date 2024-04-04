# This portion takes and prescribes jobs to different workers.
# sammatime22, 2024
import datetime
import json
import stomp
import time
import sys

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
    find_most_recent_untasked_job = "SELECT uuid, url_of_interest FROM JOB_QUEUE WHERE tasked_resource is NULL ORDER BY insert_time LIMIT 1;"

    # finds an untasked data worker
    find_untasked_data_worker = "SELECT resource_id FROM DATA_WORKERS WHERE tasked = 0 ORDER BY updated_at DESC LIMIT 1;"

    # updates a worker's status
    update_data_worker_status = "UPDATE DATA_WORKERS SET tasked=%d, current_task=%s, updated_at=NOW() WHERE resource_id=%d;"

    # updates the job status
    update_job_status = "UPDATE JOB_QUEUE SET completed=%d, complete_time=%s, tasked_resource=%d WHERE uuid=%s;"

    stomp_connection = None

    def __init__(self,  mariadb_conn_cursor):
        '''
        Initializes the Orchestrator.

        Parameters:
        ----------
        '''
        self.mariadb_conn_cursor = mariadb_conn_cursor


    def on_message(self, message):
        '''
        Pulls in messages for the Orchestrator
        '''
        # print(dir(message))
        # print(message.headers)
        message_body = json.loads(str(message.body))
        if message_body.get("dataWorker") is not None:
            return
        print("received a message " + str(message_body))
        print(str(message))
        worker_id = int(message_body["dataWorkerResponsible"])
        successful = message_body["successful"]
        completed_time = str(datetime.datetime.now()).split(".")[0]
        uuid = message_body["uuid"]
        self.mariadb_conn_cursor.execute(self.update_data_worker_status % (0, "NULL", worker_id))
        print(self.update_data_worker_status % (0, "NULL", worker_id))
        self.mariadb_conn_cursor.execute(self.update_job_status % (1, '"' + completed_time + '"', worker_id, '"' + str(uuid) + '"'))
        print(self.update_job_status % (1, '"' + completed_time + '"', worker_id, '"' + str(uuid) + '"'))


    def on_error(self, message):
        '''
        Handles message errors for the Orchestrator
        '''
        print("could not interperet message " + str(message))


    def set_stomp_connection(self, stomp_connection):
        self.stomp_connection = stomp_connection


    def check_and_send_job(self):
        '''
        Sends a store or retrieve job, depending on what is available in the DB.
        '''
        try:
            self.mariadb_conn_cursor.execute(self.find_most_recent_untasked_job)
            untasked_job_row = self.mariadb_conn_cursor.fetchall()
            # print(self.mariadb_conn_cursor.rowcount)
            if len(untasked_job_row) > 0:
                for (uuid, url_of_interest) in untasked_job_row:
                    self.mariadb_conn_cursor.execute(self.find_untasked_data_worker)
                    untasked_data_worker = self.mariadb_conn_cursor.fetchall()
                    if len(untasked_data_worker) > 0:
                        for (resource_id) in untasked_data_worker:
                            # print(resource_id[0])
                            # print(dir(self.stomp_connection))
                            self.stomp_connection.send("/data-worker", "{\"uuid\":\"" + str(uuid) + "\", \"urlOfInterest\":\"" + str(url_of_interest) + "\", \"dataWorker\":" + str(resource_id[0]) + "}")
                            # print(type(resource_id[0]))
                            # print(self.update_data_worker_status % (1, str(uuid), resource_id[0]))
                            self.mariadb_conn_cursor.execute(self.update_data_worker_status % (1, '"' + str(uuid) + '"', resource_id[0]))
                            # print("rock n roll")
                            self.mariadb_conn_cursor.execute(self.update_job_status % (0, "NULL", resource_id[0], '"' + str(uuid) + '"'))
                            # print("need something to sing about")
        except Exception as e:
            print("Error seeen: " + str(e))
            typee, obj, trace = sys.exc_info()
            print(trace.tb_lineno)


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
