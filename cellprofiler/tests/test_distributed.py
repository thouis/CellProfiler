import unittest
import os
from multiprocessing import Process
import json
import time

import zmq

from cellprofiler.modules.tests import example_images_directory
from cellprofiler.pipeline import Pipeline
from cellprofiler.distributed import Distributor, parse_json, JobTransit
import cellprofiler.preferences as cpprefs


class TestDistributor(unittest.TestCase):

    def setUp(self):
        self.address = "tcp://127.0.0.1"
        self.port = 10006

        info = self.id().split('.')[-1]
        output_finame = info + '.h5'

        ex_dir = example_images_directory()
        img_dir = os.path.join(ex_dir, "ExampleFlyImages")
        pipeline_path = os.path.join(img_dir, 'ExampleFly.cp')
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

    def _start_serving_getport(self):
        self.distributor.prepare_queue(self.pipeline, self.output_file)
        context, socket = self.distributor.prepare_socket(self.address, self.port)
        self.port = self.distributor.url.split(':')[-1]
        self.address = self.distributor.url[0:-len(self.port) - 1]

        args = (context, socket)
        server_proc = Process(target=self.distributor.run, args=args)
        return server_proc

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
                        'command':'stop'}
        client = self.context.socket(zmq.REQ)
        client.connect('%s:%s' % (self.address, self.port))
        client.send(json.dumps(stop_message), copy=False, track=True)

    def tst_start_serving(self):
        """
        Very basic test. Start server,
        make sure nothing goes wrong.
        """

        time_delay = 4
        server_proc = self._start_serving()
        server_proc.join(time_delay)
        #Server will loop forever unless it hits an error
        self.assertTrue(server_proc.is_alive())

    def tst_stop_serving(self):
        stop_message = {'type': 'command',
                        'command':'stop'}

        server_proc = self._start_serving()
        self.assertTrue(server_proc.is_alive())

        client = self.context.socket(zmq.REQ)
        client.connect('%s:%s' % (self.address, self.port))

        time_limit = 1
        tracker = client.send(json.dumps(stop_message), copy=False, track=True)
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
        still_running = server_proc.is_alive()
        if(still_running):
            server_proc.terminate()
        self.assertFalse(still_running, 'Server still running, had to terminate')


