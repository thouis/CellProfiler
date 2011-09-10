
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

# whether CP should run distributed (changed by preferences, or by command line)
force_run_distributed = False
def run_distributed():
    return (force_run_distributed or cpprefs.get_run_distributed())

class QueueDict(OrderedDict):
    """
    Dictionary we can use as a queue
    """

    def rotate(self,n=1):
        value = None
        for stp in xrange(0,n):
            key,value = self.popitem(last=False)
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

    def start_serving(self, pipeline, output_file,address = 'tcp://127.0.0.1',port = None):

        self.output_file = output_file

        # make sure createbatchfiles is not in the pipeline
        if 'CreateBatchFiles' in [module.module_name for module in pipeline.modules()]:
            # XXX - should offer to ignore?
            raise RuntimeError('CreateBatchFiles should not be used with distributed processing.')

        # duplicate pipeline
        pipeline = pipeline.copy()

        # create the image list
        image_set_list = cpi.ImageSetList()
        image_set_list.combine_path_and_file = True
        self.measurements = cpmeas.Measurements(filename = self.output_file)
        workspace = cpw.Workspace(pipeline, None, None, None, self.measurements,
                                  image_set_list)
                                  
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
        raw_pipeline_file = open(raw_pipeline_path,'r')
        pipeline_txt = raw_pipeline_file.read()
        pipeline_fd,pipeline_path = tempfile.mkstemp()     
        pipeline_file = open(pipeline_path,'w')

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
            self.work_queue[img_set_index + 1] = "%s" % self.pipeline_blob_hash

        # start serving
        self.total_jobs = image_set_list.count()
        
        self.run(address,port)
        
    def has_work(self):
        return len(self.work_queue) > 0
    
    def get_next(self):
        try:
            return self.work_queue.rotate()
        except KeyError:
            #No work left
            return None

    def get_pipeline_info(self):
        return {'path':self.pipeline_path,
                    'hash':self.pipeline_blob_hash}
    
    def report_results(self,msg):
        jobnum = msg['jobnum']
        pipeline_hash = msg['pipeline_hash']
        work_item = self.work_queue.get(jobnum,None)
        response = {'status':'failure'}
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
            self.measurements.combine_measurements(curr_meas,can_overwrite = True)
            del curr_meas

            #This can potentially cause a race condition,
            #so make sure report_results is called on the same thread/process
            #as the server.
            del self.work_queue[jobnum]
            self.jobs_finished += 1
            response = {'status':'success','remaining': len(self.work_queue)}
        return response
    
        
    def run(self,address,port):
        context = zmq.Context()
        sender = context.socket(zmq.REP)
        if(port is not None):
            self.server_URL = "%s:%s" % (address,port)
            sender.bind(self.server_URL)
        else:
            port = sender.bind_to_random_port(address)
            self.server_URL = "%s:%s" % (address,port)
        
        self.jobs_finished = 0
        
        running = True
        while running:
            #XXX Implement a timeout
            raw_msg = sender.rcv()
            msg = json.loads(raw_msg)
            response = {'status':'bad request'}
            if('type' not in msg):
                #TODO Log something
                continue
            if(msg['type'] == 'pipeline_path'):
                response = self.get_pipeline_info()
            if(msg['type'] == 'next_job'):
                job = self.get_next()
                if(job is None):
                    job = {'status':'nowork'}
                response = job
            elif((msg['type'] == 'result') and (msg.has_key('result')) ):
                response = self.report_results(msg)
            elif(msg['type'] == 'stop'):
                #Huge security risk; anybody can just tell the server to stop?
                #While we're on the subject, this may not be a secure way
                #of passing data
                response = {'status':'stopping'}
                
            sender.send(json.dumps(response))
            
            running &= self.has_work()
            
        sender.close()
        context.term()
        
        self.stop_serving()

    def stop_serving(self):
        if self.pipeline_path:
            os.unlink(self.pipeline_path)
            self.pipeline_path = None

class JobInfo(object):
    def __init__(self, base_url):
        self.base_url = base_url
        self.image_set_start = None
        self.image_set_end = None
        self.pipeline_hash = None
        self.pipeline_blob = None
        self.job_num = None
        self.context = None
        self.socket = None
        self.connected = False
        
    def _init_connection(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.base_url)
        self.connected = True

    def fetch_job(self):
        # fetch the pipeline
        self._get_pipeline_blob()
        self.pipeline_hash = hashlib.sha1(self.pipeline_blob).hexdigest()
        
        if(not self.connected):
            self._init_connection()
        self.socket.send(json.dumps({'type':'next_job'}))
        raw_msg = self.socket.recv()
        msg = json.loads(raw_msg)
        if msg.get('status','').lower() == 'nowork':
            assert False, "No work to be had..."
        self.job_num = msg.keys()[0]
        pipeline_hash = msg[self.job_num]
        image_num = self.job_num
        self.image_set_start = int(image_num)
        self.image_set_end = int(image_num)
        
        assert pipeline_hash == self.pipeline_hash, "Mismatched hash, probably out of sync with server"

    def _get_pipeline_blob(self):
        if(not self.connected):
            self._init_connection()
        self.socket.send({'type':'pipeline_path'})
        raw_msg = self.socket.recv()
        msg = json.loads(raw_msg)
        pipeline_path = msg['path']
        self.pipeline_blob = urllib2.urlopen(pipeline_path).read()

    def pipeline_stringio(self):
        if(not self.pipeline_blob):
            self._get_pipeline_blob()
        return StringIO.StringIO(zlib.decompress(self.pipeline_blob))

    def report_measurements(self, pipeline, measurements):
        meas_file = open(measurements.hdf5_dict.filename,'r+b')
        out_measurements = meas_file.read()
        if(not self.connected):
            self._init_connection()
        msg = {'type':'results','result':out_measurements}
        raw_msg = json.dumps(msg)
        self.socket.send(raw_msg)
        

def fetch_work(base_URL):
    jobinfo = JobInfo(base_URL)
    jobinfo.fetch_job()
    return jobinfo
