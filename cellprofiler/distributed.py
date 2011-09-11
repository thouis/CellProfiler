import time
import os.path
import StringIO
import zlib
import hashlib
import tempfile
import json
import urllib2
import cPickle
import base64

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
        return [key, value]

class Distributor(object):
    def __init__(self):
        self.work_server = None
        self.pipeline = None
        self.pipeline_path = None
        self.output_file = None
        self.measurements = None
        self.work_queue = QueueDict()

    def start_serving(self, pipeline, output_file,
                      address="tcp://127.0.0.1", port=5555):
        self.prepare_queue(pipeline, output_file)
        context, socket = self.prepare_socket(address, port)
        #This is blocking
        self.run(context, socket)

    def prepare_queue(self, pipeline, output_file):

        if(self.pipeline_path is not None):
            #Assume we have already prepared queue
            return

        self.output_file = output_file
        # duplicate pipeline
        pipeline = pipeline.copy()

        # make sure createbatchfiles is not in the pipeline
        exclude_mods = ['createbatchfiles', 'exporttospreadsheet']
        for ind, mod in enumerate(pipeline.modules()):
            if(mod.module_name.lower() in exclude_mods):
                print '%s cannot be used in distributed mode, removing' % (mod)
                pipeline.remove_module(ind + 1)

        # create the image list
        image_set_list = cpi.ImageSetList()
        image_set_list.combine_path_and_file = True
        self.measurements = cpmeas.Measurements(filename=self.output_file)
        workspace = cpw.Workspace(pipeline, None, None, None,
                                  self.measurements, image_set_list)

        if not pipeline.prepare_run(workspace):
            raise RuntimeError('Could not create image set list.')

        # call prepare_to_create_batch, for whatever preparation is necessary
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

        self.total_jobs = image_set_list.count()

    def prepare_socket(self, address='tcp://127.0.0.1', port=None):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        if(port is not None):
            self.url = "%s:%s" % (address, int(port))
            socket.bind(self.url)
        else:
            port = socket.bind_to_random_port(address)
            self.url = "%s:%s" % (address, port)

        return context, socket

    def run(self, context, socket):
        self.jobs_finished = 0

        print 'server running on %s' % self.url
        self.running = True
        while self.running:
            #XXX Implement a timeout
            raw_msg = socket.recv()
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
                    #job = [key,value]. value is a dict of the properties
                    response = {'id': job[0],
                                'num_remaining': self.num_remaining()}
                    response.update(job[1])
            elif((msg['type'] == 'result') and ('result' in msg)):
                response = self.report_result(msg)
            elif((msg['type']) == 'command'):
                response = self.receive_command(msg)

            socket.send(json.dumps(response))

            self.running &= (self.num_remaining() > 0)

        socket.close()
        context.term()

        self.stop_serving()

    def num_remaining(self):
        return len(self.work_queue)

    def get_next(self):
        try:
            return self.work_queue.rotate()
        except KeyError:
            #No work left
            return None

    def get_pipeline_info(self):
        return {'path': self.pipeline_path,
                    'pipeline_hash': self.pipeline_blob_hash}

    def report_result(self, msg):
        jobnum = msg['id']
        pipeline_hash = msg['pipeline_hash']
        work_item = self.work_queue.get(jobnum, None)
        #print work_item
        response = {'status': 'failure'}
        if(work_item is None):
            resp = 'work item %s not found' % (jobnum)
            print resp
            response['code'] = resp
        elif(pipeline_hash != work_item['pipeline_hash']):
            resp = "mismatched pipeline hash"
            response['code'] = resp
        else:
            #Read data, write to temp file, load into HDF5_dict instance
            raw_dat = msg['result']
            meas_str = base64.b64decode(raw_dat)
            temp_dir = os.path.dirname(self.output_file)
            temp_hdf5 = tempfile.NamedTemporaryFile(dir=temp_dir)
            temp_hdf5.write(meas_str)
            temp_hdf5.flush()
            curr_meas = cpmeas.load_measurements(filename=temp_hdf5.name)

            self.measurements.combine_measurements(curr_meas,
                                                   can_overwrite=True)
            del curr_meas
            del self.work_queue[jobnum]
            self.jobs_finished += 1
            response = {'status': 'success',
                        'num_remaining': self.num_remaining()}
        return response

    def receive_command(self, msg):
        """
        Control commands from client to server.

        For now we use these for testing, should implement
        some type of security before release
        """
        command = msg['command'].lower()
        if(command == 'stop'):
            self.running = False
            response = {'status': 'stopping'}
        elif(command == 'remove'):
            jobid = '?'
            try:
                jobid = msg['id']
                response = {'id': jobid}
                del self.work_queue[jobid]
                response['status'] = 'success'
            except KeyError, exc:
                logger.error('could not delete jobid %s: %s' % (jobid, exc))
                response['status'] = 'notfound'
        return response

    def stop_serving(self):
        self.running = False
        self.work_queue.clear()
        if 'file://' in self.pipeline_path:
            path = self.pipeline_path[len('file://')::]
        try:
            os.unlink(path)
        except OSError:
            pass
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
        self.socket.send(json.dumps({'type': 'pipeline_path'}))
        raw_msg = self.socket.recv()
        msg = parse_json(raw_msg)

        if(not msg):
            return None

        try:
            pipeline_path = msg['path']
        except KeyError:
            logger.error('path not found in response')
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
            return JobInfo(-1, -1, 'no_work', '', -1, False)

        # fetch the pipeline
        pipeline_blob = self._get_pipeline_blob()
        pipeline_hash_local = hashlib.sha1(pipeline_blob).hexdigest()

        try:
            job_num = msg['id']
            pipeline_hash_rem = msg['pipeline_hash']
            image_num = int(job_num)
            jobinfo = JobInfo(image_num, image_num,
                              pipeline_blob, pipeline_hash_local, job_num)
            jobinfo.num_remaining = msg['num_remaining']

            jobinfo.is_valid = pipeline_hash_local == pipeline_hash_rem
            if(not jobinfo.is_valid):
                logger.info("Mismatched pipeline hash")
            return jobinfo
        except KeyError, exc:
            logger.debug('KeyError: %s' % exc)
            return None

    def report_measurements(self, jobinfo, measurements):
        meas_file = open(measurements.hdf5_dict.filename, 'r+b')
        meas_str = meas_file.read()
        start = time.clock()
        send_str = base64.b64encode(meas_str)
        elapsed = time.clock() - start
        print '%2.5e sec to encode string of length %s' % (elapsed, len(meas_str))

        msg = {'type': 'result', 'result': send_str}
        msg.update(jobinfo.get_dict())
        raw_msg = json.dumps(msg)

        self.socket.send(raw_msg)
        resp = self.socket.recv()
        return parse_json(resp)

class JobInfo(object):
    def __init__(self, image_set_start, image_set_end,
                 pipeline_blob, pipeline_hash, job_num, is_valid=True,
                 num_remaining=None):
        self.image_set_start = image_set_start
        self.image_set_end = image_set_end
        self.pipeline_blob = pipeline_blob
        self.pipeline_hash = pipeline_hash
        self.job_num = job_num
        self.is_valid = is_valid
        self.num_remaining = num_remaining

    def get_dict(self):
        return {'id': self.job_num,
                'pipeline_hash': self.pipeline_hash}

    def pipeline_stringio(self):
        return StringIO.StringIO(zlib.decompress(self.pipeline_blob))

def send_with_timeout(socket, msg, timeout=5):
    tracker = socket.send(msg, copy=True, track=True)
    start_time = time.clock()
    while(not tracker.done):
        time.sleep(0.05)
        if(time.clock() - start_time > timeout):
            return None


def parse_json(raw_msg):
    try:
        msg = json.loads(raw_msg)
    except ValueError:
        logger.error('could not parse json: %s' % raw_msg)
        return None
    return msg
