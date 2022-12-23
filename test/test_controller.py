# A class to test the controller of STURL.
# sammatime22, 2022
import sys
sys.path.append('src')

import unittest
from unittest.mock import patch
from controller import Controller

class TestController(unittest.TestCase):
    '''
    A set of tests for the Controller class.

    Parameters
    ----------
    unittest.TestCase : from unittest
        Provides the Unit Test Framework
    '''

    # A mocked MariaDB Connection
    class MockMariaDBConn:
        '''
        A mocked MariaDB Connection
        '''

        def __init__(self):
            '''
            No instructions for the initializer
            '''
            pass

        def execute(self, input_string):
            '''
            This method mocks the execute command of a MariaDB connection, 
            returning a string with the same structure as what was passed.

            Parameters:
            ----------
            input_string : string
                The string provided to the method

            Return:
            ----------
            input_string : string
                An untouched input string
            '''
            return input_string


    class MockStompMessage:

        header = None

        body = None

        def __init__(self, header, body):
            self.header = header
            self.body = body


    # for 1s iteration period (for tests 3 and 4)
    mock_max_iterator = 1

    mock_stomp_conn = None

    mock_mariadb_conn = MockMariaDBConn()

    test_controller = Controller(mock_max_iterator, mock_stomp_conn, mock_mariadb_conn)

    def test_01_request_data_successful(self):
        '''
        Tests the appropriate response provided request for data is placed on the queue.
        '''
        uuid_returned, success = self.test_controller.request_data("https://www.milkshake.com")

        assert uuid_returned is not None
        assert len(str(uuid_returned)) == 36
        assert success == True


    def test_02_request_data_unsuccessful(self):
        '''
        Tests the appropriate response provided request for data is not placed on the queue.
        '''
        with patch('uuid.uuid4') as mock_uuid_generation:
            mock_uuid_generation.side_effect = [Exception]
            uuid_returned, success = self.test_controller.request_data("https://www.milkshake.com")

            assert len(mock_uuid_generation.mock_calls) == 1
            assert uuid_returned == None
            assert success == False


    def test_03_collect_report_successful(self):
        '''
        Tests the appropriate response provided a report can be collected.
        '''
        self.test_controller.on_message(self.MockStompMessage("head", "{\"uuid\": \"1234\", \"stuff\": 2}"))
        message, success = self.test_controller.collect_report("1234")
        assert message is not None
        assert success == True


    def test_04_collect_report_unsuccessful(self):
        '''
        Tests the appropriate response provided a report cannot be collected.
        '''
        message_returned, success = self.test_controller.collect_report("")
        assert message_returned == None
        assert success == False
