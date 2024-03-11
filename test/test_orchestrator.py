# A class to test the orchestrator of STURL.
# sammatime22, 2024
import sys
sys.path.append('src')

import unittest
from unittest.mock import patch, call
from orchestrator import Orchestrator

class TestOrchestrator(unittest.TestCase):
    # '''
    # A set of tests for the Orchestrator class.

    # Parameters:
    # ----------
    # unittest.TestCase : from unittest
    #     Provides the Unit Test Framework
    # '''

    # def break_from_loop(self):
    #     '''
    #     Just a mock method to break from the overarching main_loop method.
    #     '''
    #     self.test_orchestrator.continue_running = False

    # def fake_execute(input_string):
    #     '''
    #     Fakes the execute method off of the mariadb_conn to test the appropriate input
    #     args have been provided

    #     Parameters:
    #     ----------
    #     input_string : string
    #         A fake input string provided for no reason other than providing the string
    #     '''
    #     # print(input_string)
    #     # print("abcd")


    # # mock number of days after which items should be removed from the DB
    # mock_remove_after_n_days = 1

    # test_orchestrator = Orchestrator(mock_remove_after_n_days)

    # def test_01_store_job_request_sent(self):
    #     '''
    #     Tests the appropriate message is sent provided a store job is tasked.
    #     '''
    #     pass

    # def test_02_retrieve_job_request_sent(self):
    #     '''
    #     Tests the appropriate message is sent provided a retrieve job is tasked.
    #     '''
    #     pass

    # # @patch('builtins.print')
    # def test_03_remove_old_jobs_from_queue(self):
    #     '''
    #     Tests that the Orchestrator will remove old jobs after some period of time.
    #     '''
    #     with patch('orchestrator.Orchestrator.mariadb_conn') as mock_mariadb_conn:
    #         mock_mariadb_conn.execute = self.fake_execute
    #         self.test_orchestrator.delete_old_jobs()
    #         # assert len(mock_print.mock_calls) == 1
    #         # assert mock_print.mock_calls == [call("DELETE FROM JOB_QUEUE WHERE DATEDIFF(day, getdate(), insert_time) < 1;")]
    #         # assert mock_print.mock_calls == []


    # def test_04_main_loop_test(self):
    #     '''
    #     Tests that the main loop of the Orchestrator touches on all necessary methods.
    #     '''
    #     with patch('orchestrator.Orchestrator.check_and_send_job') as mock_check_and_send_job:
    #         with patch('orchestrator.Orchestrator.delete_old_jobs') as mock_delete_old_jobs:
    #             mock_delete_old_jobs.side_effect = self.break_from_loop
    #             self.test_orchestrator.main_loop()

    #             assert len(mock_check_and_send_job.mock_calls) == 1
    #             assert len(mock_delete_old_jobs.mock_calls) == 1
