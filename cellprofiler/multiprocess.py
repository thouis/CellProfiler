"""
Designed to act as a server for utilizing multiple cores
on a machine and processing in parallel
"""

import multiprocessing
import StringIO
import time
import logging

from distributed import JobTransit

from cellprofiler.pipeline import Pipeline
import cellprofiler.preferences as cpprefs

# whether CP should run multiprocessing
# changed by preferences, or by command line
force_run_multiprocess = False
def run_multiprocess():
    return (force_run_multiprocess or cpprefs.get_run_multiprocess())

def worker_looper(url):
    has_work = True
    responses = []
    transit = JobTransit(url)
    while has_work:
        jobinfo = transit.fetch_job()
        if(jobinfo and jobinfo.is_valid):
            measurements = single_job(jobinfo)
            response = transit.report_measurements(jobinfo, measurements)
            responses.append(response)
        else:
            has_work = False
    return response

def single_job(jobinfo):
        pipeline = Pipeline()
        try:
            pipeline.load(jobinfo.pipeline_stringio())
            image_set_start = jobinfo.image_set_start
            image_set_end = jobinfo.image_set_end
        except:
            logging.root.error("Can't parse pipeline for distributed work.",
                               exc_info=True)
            return [jobinfo.job_num, 'FAILURE']

        measurements = pipeline.run(image_set_start=image_set_start,
                                    image_set_end=image_set_end)
        return measurements

def run_multiple_workers(url, num_workers=None):
    """
    Run multiple local workers which will attempt to
    retrieve work from the provided URL.

    Does not block, starts up a pool of workers.

    Returns
    -----------
    pool - multiprocessing.Pool
        The pool doing work
    """
    if(not num_workers):
        num_workers = multiprocessing.cpu_count()

    pool = multiprocessing.Pool(num_workers)

    urls = [url] * num_workers
    for url in urls:
        pool.apply_async(worker_looper, args=(url))
    #Note: The results will not be available immediately
    #because we haven't joined the pool
    return pool
#
#def _start_serving_headless(pipeline,output_file_path,
#                            address, port):
#    distributor = Distributor()
#    distributor.start_serving(pipeline, output_file_path)
#    print "serving at ", distributor.server_URL
#    return distributor
#
#def run_pipeline_headless(pipeline,output_file_path,
#                            address, port):
#    distributor = _start_serving_headless(pipeline, output_file_path,
#                                          address, port)
#    #Start workers
#    pool = run_multiple_workers(distributor.server_URL)
#
#    running_pipeline = distributor.run_with_yield()
#    for ghost in running_pipeline:
#        time.sleep(0.1)
#        pass

if __name__ == '__main__':
    pass
