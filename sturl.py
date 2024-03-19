# The portion of the application that starts up threads and such.
# sammatime22, 2024
import asyncio
import yaml
import sys
import stomp
import mariadb
import pymongo
sys.path.append('src')
from data_worker import DataWorker
from orchestrator import Orchestrator
from controller import Controller

CONFIG_LOCATION = "config/sturl_config.yaml"
DATA_WORKERS = "data_workers"
CONTROLLER = "controller"
NUM_OF_WORKERS = "num_of_workers"
TIMEOUT = "timeout"
INTERFACE = "Sturl Application"
STOMP = "stomp"
USER = "root"
PASSWORD = "mariadbpw"
DATA_WORKER_TOPIC_BASE = "data-worker-%d"
MARIA_DB_IP = "0.0.0.0"
MARIA_DB_PORT = "55000"
MARIA_DB_DATABASE = "sturl"
MONGO_DB_ADDRESS = "mongodb://localhost:27017"


def stomp_factory(listener, worker_number):
    '''
    Given a worker number, sets up a connection to receive jobs.
    '''
    print("building connection for worker " + str(worker_number))
    worker = stomp.Connection([('0.0.0.0', 63636)])
    worker.set_listener('', listener)
    worker.connect(sturl_config[STOMP]["user"], sturl_config[STOMP]["password"], wait=False)
    worker.subscribe(destination=DATA_WORKER_TOPIC_BASE % worker_number, id=worker_number, ack='auto')


def maria_db_factory():
    '''
    Returns a connection cursor to mariadb
    '''
    conn = mariadb.connect(user=USER, password=PASSWORD, host=MARIA_DB_IP, port=int(MARIA_DB_PORT), database=MARIA_DB_DATABASE)
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

    for i in range(0, sturl_config[DATA_WORKERS][NUM_OF_WORKERS]):
        stomp_factory(DataWorker(i, mongo_db_factory()), i)
        stomp_factory(orchestrat_or, i)

    asyncio.run(orchestrat_or.main_loop())

    controll_er = Controller(maria_db_factory(), mongo_db_factory())

    interface = Flask(self.INTERFACE)

    @interface.route('/request', methods=['POST'])
    def request_data():
        '''
        Uses the URL provided in the POST request to kick off
        a job in the STURL backend.
        '''
        return controll_er.request_data(request.form['url'])

    @interface.route('/response', methods=['GET'])
    def retrieve_data():
        '''
        A long-poll to pick up the report from MongoDB.
        '''
        return controll_er.retrieve_data(request.form['uuid'])

    interface.run()

    print("Interface Activated")
    print("STURL Started Healthily")
