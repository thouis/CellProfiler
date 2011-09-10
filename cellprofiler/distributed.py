
import os.path
import StringIO
import zlib
import hashlib
import tempfile
import json
import urllib2

import logging
from collections import OrderedDict
logger = logging.getLogger(__name__)

import zmq

import cellprofiler.preferences as cpprefs
import cellprofiler.cpimage as cpi
import cellprofiler.workspace as cpw
import cellprofiler.measurements as cpmeas

#Whether CP should run distributed (changed by preferences or command line)
force_run_distributed = False
def run_distributed():
    return (force_run_distributed or cpprefs.get_run_distributed())

class QueueDict(OrderedDict):
    """
    Dictionary we can use as a queue
    """

    def rotate(self, n=1):
        value = None
        for stp in xrange(0, n):
            key, value = self.popitem(last=False)
            self[key] = value
        return value

class Distributor(object):
    def __init__(self):
        self.work_server = None
        self.pipeline = None
        self.pipeline_path = None
        self.output_file = None
        self.measurements = None
        self.work_queue = QueueDict()

    def start_serving(self, pipeline, output_file,
                      address='tcp://127.0.0.1', port=None):

        self.output_file = output_file

        # make sure createbatchfiles is not in the pipeline
        mod_names = [mod.module_name for mod in pipeline.modules()]
        if 'CreateBatchFiles' in mod_names:
            # XXX - should offer to ignore?
            raise RuntimeError('CreateBatchFiles should not '
                'be used with distributed processing.')

        # duplicate pipeline
        pipeline = pipeline.copy()

        # create the image list
        image_set_list = cpi.ImageSetList()
        image_set_list.combine_path_and_file = True
        self.measurements = cpmeas.Measurements(filename=self.output_file)
        workspace = cpw.Workspace(pipeline, None, None, None,
                                  self.measurements, image_set_list)

        if not pipeline.prepare_run(workspace):
            raise RuntimeError('Could not create image set list for distributed processing.')

        # call prepare_to_create_batch, for whatever preparation may be necessary
        # hopefully none
        #pipeline.prepare_to_create_batch(workspace, lambda s: s)

        # add a CreateBatchFiles module at the end of the pipeline,
        # and set it up for saving the pipeline state
        module = pipeline.instantiate_module('CreateBatchFiles')
        module.module_num = len(pipeline.modules()) + 1
        pipeline.add_module(module)
        module.wants_default_output_directory.set_value(True)
        module.remote_host_is_windows.set_value(False)
        module.batch_mode.set_value(False)
        module.distributed_mode.set_value(True)

        #TODO This is really not ideal
        #save and compress the pipeline
        #This saves the data directly on disk, uncompressed
        raw_pipeline_path = module.save_pipeline(workspace)
        #Read it back into memory
        raw_pipeline_file = open(raw_pipeline_path, 'r')
        pipeline_txt = raw_pipeline_file.read()
        pipeline_fd, pipeline_path = tempfile.mkstemp()
        pipeline_file = open(pipeline_path, 'w')

        pipeline_blob = zlib.compress(pipeline_txt)
        pipeline_file.write(pipeline_blob)
        pipeline_file.close()
        self.pipeline_path = 'file://%s' % (pipeline_path)

        # we use the hash to make sure old results don't pollute new
        # ones, and that workers are fetching what they expect.
        self.pipeline_blob_hash = hashlib.sha1(pipeline_blob).hexdigest()

        # add jobs for each image set
        #XXX Maybe use guid instead of img_set_index?
        for img_set_index in range(image_set_list.count()):
            self.work_queue[img_set_index + 1] = \
                {'pipeline_hash': self.pipeline_blob_hash}

        # start serving
        self.total_jobs = image_set_list.count()

        self.run(address, port)

    def has_work(self):
        return len(self.work_queue) > 0

    def get_next(self):
        try:
            return self.work_queue.rotate()
        except KeyError:
            #No work left
            return None

    def get_pipeline_info(self):
        return {'path': self.pipeline_path,
                    'pipeline_hash': self.pipeline_blob_hash}

    def report_results(self, msg):
        jobnum = msg['id']
        pipeline_hash = msg['pipeline_hash']
        work_item = self.work_queue.get(jobnum, None)
        response = {'status': 'failure'}
        if(work_item is None):
            resp = 'work item %s not found' % (jobnum)
            print resp
            response['code'] = resp
        elif(pipeline_hash != work_item):
            # out of date result?
            resp = "ignored mismatched pipeline hash", pipeline_hash, work_item
            response['code'] = resp
        else:
            #Read data, write to temp file, load into HDF5_dict instance
            raw_dat = msg['measurements']
            meas_str = zlib.decompress(raw_dat)

            temp_hdf5 = tempfile.NamedTemporaryFile(dir=os.path.dirname(self.output_file))
            temp_hdf5.write(meas_str)
            temp_hdf5.flush()

            curr_meas = cpmeas.load_measurements(filename=temp_hdf5.name)
            self.measurements.combine_measurements(curr_meas, can_overwrite=True)
            del curr_meas

            #This can potentially cause a race condition,
            #so make sure report_results is called on the same thread/process
            #as the server.
            del self.work_queue[jobnum]
            self.jobs_finished += 1
            response = {'status': 'success', 'remaining': len(self.work_queue)}
        return response

    def receive_control(self, msg):
        """
        Control commands from client to server.

        For now we use these for testing, should implement
        some type of security before release
        """
        command = msg['command'].lower()
        if(command == 'stop'):
            self.running = False
            response = {'status':'stopping'}
        elif(command == 'remove'):
            jobid = '?'
            try:
                jobid = msg['jobid']
                response = {'id':jobid}
                del self.work_queue[jobid]
                response['status'] = 'success'
            except KeyError, exc:
                logger.log('could not delete jobid %s: %s' % (jobid, exc))
                response['status'] = 'notfound'
        return response

    def run(self, address, port):
        context = zmq.Context()
        sender = context.socket(zmq.REP)
        if(port is not None):
            self.server_URL = "%s:%s" % (address, port)
            sender.bind(self.server_URL)
        else:
            port = sender.bind_to_random_port(address)
            self.server_URL = "%s:%s" % (address, port)

        self.jobs_finished = 0

        self.running = True
        while self.running:
            #XXX Implement a timeout
            raw_msg = sender.rcv()
            msg = parse_json(raw_msg)

            response = {'status': 'bad request'}
            if((msg is None) or ('type' not in msg)):
                #TODO Log something
                pass
            elif(msg['type'] == 'pipeline_path'):
                response = self.get_pipeline_info()
            elif(msg['type'] == 'next_job'):
                job = self.get_next()
                if(job is None):
                    response = {'status': 'nowork'}
                else:
                    response = {'id': job[0], 'pipeline_hash': job[1]}
            elif((msg['type'] == 'result') and ('result' in msg)):
                response = self.report_results(msg)
            elif((msg['type']) == 'control'):
                response = self.receive_control(msg)

            sender.send(json.dumps(response))

            self.running &= self.has_work()

        sender.close()
        context.term()

        self.stop_serving()

    def stop_serving(self):
        self.running = False
        self.work_queue.clear()
        if self.pipeline_path:
            os.unlink(self.pipeline_path)
            self.pipeline_path = None

class JobTransit(object):
    def __init__(self, url, context=None, socket=None):
        self.url = url
        self.context = context
        self.socket = socket
        if(self.socket is None):
            self._init_connection()

    def _init_connection(self):
        if(self.context is None):
            self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.url)

    def _get_pipeline_blob(self):
        self.socket.send({'type': 'pipeline_path'})
        raw_msg = self.socket.recv()
        msg = parse_json(raw_msg)

        if(not msg):
            return None

        try:
            pipeline_path = msg['path']
        except KeyError:
            logger.log('path not found in response')
            return None

        return urllib2.urlopen(pipeline_path).read()

    def fetch_job(self):
        self.socket.send(json.dumps({'type': 'next_job'}))
        raw_msg = self.socket.recv()
        msg = parse_json(raw_msg)
        if(not msg):
            return None

        if msg.get('status', '').lower() == 'nowork':
            print "No work to be had."
            return JobInfo(-1, -1, 'no_work', '', -1)

        # fetch the pipeline
        pipeline_blob = self._get_pipeline_blob()
        pipeline_hash = hashlib.sha1(pipeline_blob).hexdigest()

        try:
            job_num = msg['id']
            pipeline_hash = msg['pipeline_hash']
            image_num = int(job_num)
            jobinfo = JobInfo(image_num, image_num, pipeline_blob, pipeline_hash, job_num)
            valid = pipeline_hash == self.pipeline_hash
            if(not valid):
                logger.log("Mismatched hash, probably out of sync with server")
            return jobinfo
        except KeyError, exc:
            logger.log('KeyError: %s' % exc)
            return None

    def report_measurements(self, pipeline, measurements):
        meas_file = open(measurements.hdf5_dict.filename, 'r+b')
        out_measurements = meas_file.read()

        msg = {'type': 'results', 'result': out_measurements}
        raw_msg = json.dumps(msg)
        self.socket.send(raw_msg)
        resp = self.socket.recv()
        return self._parse_response(resp)

class JobInfo(object):
    def __init__(self, image_set_start, image_set_end,
                 pipeline_blob, pipeline_hash, job_num):
        self.image_set_start = image_set_start
        self.image_set_end = image_set_end
        self.pipeline_blob = pipeline_blob
        self.pipeline_hash = pipeline_hash
        self.job_num = job_num

    def pipeline_stringio(self):
        return StringIO.StringIO(zlib.decompress(self.pipeline_blob))


def parse_json(self, raw_msg):
    try:
        msg = json.loads(raw_msg)
    except ValueError:
        logger.log('could not parse json: %s' % raw_msg)
        return None
    return msg
