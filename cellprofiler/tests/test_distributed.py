import unittest
import os
import json
import time

import zmq
import numpy as np

from cellprofiler.modules.tests import example_images_directory
from cellprofiler.pipeline import Pipeline
from cellprofiler.distributed import JobTransit, JobInfo, Distributor
from cellprofiler.distributed import  send_with_timeout, parse_json
import cellprofiler.preferences as cpprefs
from cellprofiler.multiprocess import single_job, worker_looper, run_pipeline_headless
import cellprofiler.measurements as cpmeas
from test_Measurements import compare_measurements

test_dir = os.path.dirname(os.path.abspath(__file__))
test_data_dir = os.path.join(test_dir, 'data')

class TestDistributor(unittest.TestCase):
    def setUp(self):
        self.address = "tcp://127.0.0.1"
        self.port = None

        info = self.id().split('.')[-1]
        output_finame = info + '.h5'

        ex_dir = example_images_directory()
        self.img_dir = os.path.join(ex_dir, "ExampleWoundHealingImages")
        img_dir = self.img_dir
        pipeline_path = os.path.join(img_dir, 'ExampleWoundHealing.cp')
        self.output_file = os.path.join(img_dir, output_finame)

        self.pipeline = Pipeline()
        self.pipeline.load(pipeline_path)

        self.distributor = Distributor(self.pipeline, self.output_file,
                                       self.address, self.port)

        #Might be better to write these paths into the pipeline
        self.old_image_dir = cpprefs.get_default_image_directory()
        cpprefs.set_default_image_directory(img_dir)

        self.context = zmq.Context()
        self.procs = []

    def tearDown(self):
        self.address = None
        self.port = None
        self.output_file = None

        self.pipeline = None
        self.distributor = None
        #This hangs if we haven't closed all the sockets
        #self.context.term()
        for proc in self.procs:
            if(proc.is_alive()):
                print 'terminating ' + proc.name()
                proc.terminate()

        cpprefs.set_default_image_directory(self.old_image_dir)

    def _start_serving(self, port=None):
        url = self.distributor.start_serving()
        return self.distributor.server_proc, url

    def _stop_serving_clean(self, url=None):
        stop_message = {'type': 'command',
                        'command': 'stop'}
        client = self.context.socket(zmq.REQ)
        if(url is None):
            url = '%s:%s' % (self.address, self.port)
        client.connect(url)
        send_with_timeout(client, json.dumps(stop_message))
        client.close()

    def test_start_serving(self):
        """
        Very basic test. Start server,
        make sure nothing goes wrong.
        """

        time_delay = 0.1
        url = self._start_serving()
        server_proc = self.distributor.server_proc
        server_proc.join(time_delay)
        #Server will loop forever unless it hits an error
        self.assertTrue(server_proc.is_alive())

    def test_stop_serving(self):
        stop_message = {'type': 'command',
                        'command': 'stop'}

        server_proc, url = self._start_serving()
        self.assertTrue(server_proc.is_alive())

        client = self.context.socket(zmq.REQ)
        client.connect(url)

        time_limit = 1
        tracker = client.send(json.dumps(stop_message),
                              copy=False, track=True)
        start_time = time.clock()
        while(not tracker.done):
            elapsed = time.clock() - start_time
            self.assertFalse(elapsed > time_limit,
                             'Timeout while sending message to server')
            time.sleep(0.1)

        resp = parse_json(client.recv())
        self.assertTrue('status' in resp)
        self.assertTrue(resp['status'] == 'stopping')

        #Race condition here. We resolve by waiting awhile,
        #give the server some time to stop
        time.sleep(1)
        self.assertFalse(server_proc.is_alive())
        client.close()

    def test_get_work_01(self):
        """
        Prepare a queue of work.
        Keep getting work but don't report results,
        make sure jobs are served over and over
        """

        self.distributor._prepare_queue()
        num_jobs = self.distributor._total_jobs
        del self.distributor
        self.setUp()

        server_proc, url = self._start_serving()
        self.assertTrue(server_proc.is_alive())

        num_trials = 100
        fetcher = JobTransit(url)

        for tri in xrange(num_trials):
            job = fetcher.fetch_job()
            self.assertIsNotNone(job)
            self.assertTrue(job.is_valid)
            exp_tri = (tri % num_jobs) + 1
            act_tri = job.job_num
            self.assertEqual(exp_tri, act_tri, 'Job Numbers do not match')

        self._stop_serving_clean(url)

    def test_remove_work(self):
        """
        Get jobs and delete them from server (do not report results)
        """
        self.distributor._prepare_queue()
        num_jobs = self.distributor._total_jobs
        del self.distributor
        self.setUp()

        server_proc, url = self._start_serving()
        self.assertTrue(server_proc.is_alive())
        fetcher = JobTransit(url)

        controller = self.context.socket(zmq.REQ)
        controller.connect(url)

        received = set()
        while True:
            job = fetcher.fetch_job()
            is_valid = job.is_valid
            if(not is_valid):
                break
            msg = {'type': 'command',
                   'command': 'remove',
                   'id': job.job_num}
            controller.send(json.dumps(msg))
            rec = parse_json(controller.recv())
            self.assertTrue(rec['status'] == 'success')
            received.add(job.job_num)
            if(job.num_remaining == 1):
                break

        act_num_jobs = len(received)
        self.assertTrue(num_jobs, act_num_jobs)
        expected = set(xrange(1, num_jobs + 1))
        self.assertTrue(expected, received)

    @np.testing.decorators.slow
    @unittest.skip('')
    def test_single_job(self):
        server_proc, url = self._start_serving()
        transit = JobTransit(url)
        jobinfo = transit.fetch_job()
        measurement = single_job(jobinfo)
        #print measurement
        self._stop_serving_clean(url)

    @np.testing.decorators.slow
    @unittest.skip('lengthy test')
    def test_worker_looper(self):
        server_proc, url = self._start_serving()
        responses = worker_looper(url)
        self._stop_serving_clean(url)
        for response in responses:
            self.assertEqual(response['status'], 'success')
        self.assertFalse(server_proc.is_alive())

    @np.testing.decorators.slow
    def test_report_measurements(self):
        server_proc, url = self._start_serving()

        meas_file = os.path.join(test_data_dir, 'CpmeasurementsGhqeaL.hdf5')
        curr_meas = cpmeas.load_measurements(filename=meas_file)
        transit = JobTransit(url)
        jobinfo = JobInfo(0, 0, None, None, 1)

        response = transit.report_measurements(jobinfo, curr_meas)
        self.assertTrue('code' in response)
        self.assertTrue('mismatched pipeline hash' in response['code'])
        self._stop_serving_clean(url)

    @np.testing.decorators.slow
    @unittest.skip('lengthy test')
    def test_wound_healing(self):
        ex_dir = example_images_directory()
        pipeline_path = os.path.join(ex_dir, 'ExampleWoundHealingImages', 'ExampleWoundHealing.cp')
        ref_data_path = os.path.join(test_data_dir, 'ExampleWoundHealingImages', 'ExampleWoundHealing_ref.h5')
        output_file_path = os.path.join(test_data_dir, 'output', 'test_wound_healing.h5')

        self.tst_pipeline_multi(pipeline_path, ref_data_path, output_file_path)

    def tst_pipeline_multi(self, pipeline_path, ref_data_path, output_file_path):
        pipeline = Pipeline()
        pipeline.load(pipeline_path)

        run_pipeline_headless(pipeline, output_file_path, self.address, self.port)

        ref_meas = cpmeas.load_measurements(ref_data_path)
        test_meas = cpmeas.load_measurements(output_file_path)

        compare_measurements(ref_meas, test_meas, check_feature)

def check_feature(feat_name):
        fnl = feat_name.lower()
        ignore = ['executiontime', 'pathname', 'filename', 'pipeline_pipeline',
                  'group_index']
        for igflag in ignore:
            if igflag in fnl:
                return False
        return True

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDistributor('test_worker_looper'))
    return suite

if __name__ == "__main__":
    unittest.main()
    #unittest.TextTestRunner(verbosity=2).run(suite())
