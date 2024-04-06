# The portion of the application that starts up threads and such.
# sammatime22, 2024
import threading
import yaml
import sys
import stomp
import mariadb
import pymongo
sys.path.append('src')
from data_worker import DataWorker
from orchestrator import Orchestrator
from controller import Controller
from flask import Flask, request


CONFIG_LOCATION = "config/sturl_config.yaml"
DATA_WORKERS = "data_workers"
CONTROLLER = "controller"
NUM_OF_WORKERS = "num_of_workers"
TIMEOUT = "timeout"
INTERFACE = "Sturl Application"
STOMP = "stomp"
USER = "root"
PASSWORD = "mariadbpw"
DATA_WORKER_TOPIC_BASE = "/data-worker"
MARIA_DB_IP = "0.0.0.0"
MARIA_DB_PORT = "55000"
MARIA_DB_DATABASE = "sturl"
MONGO_DB_ADDRESS = "mongodb://docker:mongopw@localhost:55003"


def stomp_factory(listener, worker_number):
    '''
    Given a worker number, sets up a connection to receive jobs.
    '''
    print("building connection for worker " + str(worker_number))
    worker = stomp.Connection([('0.0.0.0', 63636)], heartbeats=(4000, 4000))
    worker.set_listener('', listener)
    worker.connect(sturl_config[STOMP]["user"], sturl_config[STOMP]["password"], wait=True, headers = {'client-id': 'clientname'})
    worker.subscribe(destination=DATA_WORKER_TOPIC_BASE, id=worker_number)
    listener.set_stomp_connection(worker)


def maria_db_factory():
    '''
    Returns a connection cursor to mariadb
    '''
    conn = mariadb.connect(user=USER, password=PASSWORD, host=MARIA_DB_IP, port=int(MARIA_DB_PORT), database=MARIA_DB_DATABASE)
    conn.autocommit = True
    return conn.cursor()


def mongo_db_factory():
    '''
    Returns a client to MongoDB
    '''
    return pymongo.MongoClient(MONGO_DB_ADDRESS)


if __name__ == '__main__':
    with open(CONFIG_LOCATION, 'r') as sturl_config_file:
        sturl_config = yaml.safe_load(sturl_config_file)

    orchestrat_or = Orchestrator(maria_db_factory())

    mariadb_cursor = maria_db_factory()

    mariadb_cursor.execute("DELETE FROM DATA_WORKERS;")
    mariadb_cursor.execute("DELETE FROM JOB_QUEUE;")

    stomp_factory(orchestrat_or, sturl_config[DATA_WORKERS][NUM_OF_WORKERS])

    for i in range(0, sturl_config[DATA_WORKERS][NUM_OF_WORKERS]):
        mariadb_cursor.execute("INSERT INTO DATA_WORKERS (resource_id, tasked) VALUES (%d, 0);" % i)
        stomp_factory(DataWorker(i, mongo_db_factory()), i)

    orchestrator_thread = threading.Thread(target=orchestrat_or.main_loop)
    orchestrator_thread.start()

    controll_er = Controller(maria_db_factory(), mongo_db_factory())

    interface = Flask(INTERFACE)

    @interface.route('/request', methods=['POST'])
    def request_data():
        '''
        Uses the URL provided in the POST request to kick off
        a job in the STURL backend.
        '''
        return str(controll_er.request_data(request.form['url'], request.remote_addr))

    @interface.route('/response', methods=['GET'])
    def retrieve_data():
        '''
        A long-poll to pick up the report from MongoDB.
        '''
        return controll_er.collect_report(request.args['uuid'])

    interface.run()
