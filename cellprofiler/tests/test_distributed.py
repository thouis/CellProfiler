import unittest
import os
from multiprocessing import Process
import json
import time

import zmq

from cellprofiler.modules.tests import example_images_directory
from cellprofiler.pipeline import Pipeline
from cellprofiler.distributed import JobTransit, JobInfo
from cellprofiler.distributed import  Distributor, parse_json
import cellprofiler.preferences as cpprefs
from cellprofiler.multiprocess import single_job, worker_looper
import cellprofiler.measurements as cpmeas

test_dir = os.path.dirname(os.path.abspath(__file__))
test_data_dir = os.path.join(test_dir, 'data')

class TestDistributor(unittest.TestCase):

    def setUp(self):
        self.address = "tcp://127.0.0.1"
        self.port = 5580

        info = self.id().split('.')[-1]
        output_finame = info + '.h5'

        ex_dir = example_images_directory()
        self.img_dir = os.path.join(ex_dir, "ExampleWoundHealingImages")
        img_dir = self.img_dir
        pipeline_path = os.path.join(img_dir, 'ExampleWoundHealing.cp')
        self.output_file = os.path.join(img_dir, output_finame)

        self.pipeline = Pipeline()
        self.pipeline.load(pipeline_path)

        self.distributor = Distributor()

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
        self.context.term()
        for proc in self.procs:
            proc.terminate()

        cpprefs.set_default_image_directory(self.old_image_dir)

    def _start_serving(self, port=None):
        if(port is None):
            port = self.port
        args = (self.pipeline, self.output_file, self.address, port)
        server_proc = Process(target=self.distributor.start_serving, args=args)
        server_proc.start()
        self.procs.append(server_proc)
        return server_proc

    def _stop_serving_clean(self):
        stop_message = {'type': 'command',
                        'command': 'stop'}
        client = self.context.socket(zmq.REQ)
        client.connect('%s:%s' % (self.address, self.port))
        client.send(json.dumps(stop_message), copy=False, track=True)

    def test_start_serving(self):
        """
        Very basic test. Start server,
        make sure nothing goes wrong.
        """

        time_delay = 4
        server_proc = self._start_serving()
        server_proc.join(time_delay)
        #Server will loop forever unless it hits an error
        self.assertTrue(server_proc.is_alive())

    def test_stop_serving(self):
        stop_message = {'type': 'command',
                        'command': 'stop'}

        server_proc = self._start_serving()
        self.assertTrue(server_proc.is_alive())

        client = self.context.socket(zmq.REQ)
        client.connect('%s:%s' % (self.address, self.port))

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

    def test_get_work_01(self):
        """
        Prepare a queue of work.
        Keep getting work but don't report results,
        make sure jobs are served over and over
        """

        self.distributor.prepare_queue(self.pipeline, self.output_file)
        num_jobs = self.distributor.total_jobs
        del self.distributor
        self.setUp()

        server_proc = self._start_serving()
        self.assertTrue(server_proc.is_alive())

        num_trials = 100
        url = "%s:%s" % (self.address, self.port)
        fetcher = JobTransit(url)

        for tri in xrange(num_trials):
            job = fetcher.fetch_job()
            self.assertIsNotNone(job)
            self.assertTrue(job.is_valid)
            exp_tri = (tri % num_jobs) + 1
            act_tri = job.job_num
            self.assertEqual(exp_tri, act_tri, 'Job Numbers do not match')

        self._stop_serving_clean()

    def test_remove_work(self):
        """
        Get jobs and delete them from server (do not report results)
        """
        self.distributor.prepare_queue(self.pipeline, self.output_file)
        num_jobs = self.distributor.total_jobs
        del self.distributor
        self.setUp()

        server_proc = self._start_serving()
        self.assertTrue(server_proc.is_alive())
        url = "%s:%s" % (self.address, self.port)
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

    def test_single_job(self):
        self._start_serving()
        url = '%s:%s' % (self.address, self.port)
        transit = JobTransit(url)
        jobinfo = transit.fetch_job()
        measurement = single_job(jobinfo)
        #print measurement
        self._stop_serving_clean()

    #@unittest.expectedFailure
    @unittest.skip('lengthy test and expected failure')
    def test_worker_looper(self):
        self._start_serving()
        url = '%s:%s' % (self.address, self.port)
        responses = worker_looper(url)
        print responses
        self._stop_serving_clean()

    def test_report_measurements(self):
        self._start_serving()
        url = '%s:%s' % (self.address, self.port)

        meas_file = os.path.join(test_data_dir, 'Cpmeasurementsam6C7Z.hdf5')
        curr_meas = cpmeas.load_measurements(filename=meas_file)
        transit = JobTransit(url)
        jobinfo = JobInfo(0, 0, None, None, 1)

        response = transit.report_measurements(jobinfo, curr_meas)
        self.assertTrue('code' in response)
        self.assertTrue('mismatched pipeline hash' in response['code'])
        self._stop_serving_clean()

    #@unittest.expectedFailure
    @unittest.skip('lengthy test and expected failure')
    def test_wound_healing(self):
        self._start_serving()
        url = '%s:%s' % (self.address, self.port)
        responses = worker_looper(url)
        expected_meas_fi = os.path.join(test_data_dir, 'WoundHealingResults.h5')
        act_meas_fi = self.output_file
        exp_meas = cpmeas.load(filename=expected_meas_fi)
        act_meas = cpmeas.load(filename=act_meas_fi)
        from cellprofiler.tests.test_Measurements import tst_compare_measurements
        tst_compare_measurements(exp_meas, act_meas)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDistributor('test_worker_looper'))
    return suite

if __name__ == "__main__":
    unittest.main()
    #suite = suite()
    #unittest.TextTestRunner(verbosity=2).run(suite())
