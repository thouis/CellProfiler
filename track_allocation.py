import numpy as np
import inspect
import time

class AllocationTracer(object):
    def __init__(self, threshold=0, basedir=None):
        '''trace numpy allocations of size threshold bytes or more.'''

        self.threshold = threshold
        self.basedir = basedir or ''

        # The total number of bytes currently allocated with size above threshold
        self.total_bytes = 0

        # We buffer requests line by line and move them into the allocation trace when a new line occurs
        self.current_line = None
        self.pending_allocations = []

        self.blocksizes = {}  # used to handle calls to realloc()

        # list of (lineinfo, bytes allocated, bytes freed, # allocations, # frees, maximum memory usage, long-lived bytes allocated, timestamp)
        self.allocation_trace = []

    def start(self):
        np.core.multiarray.trace_data_allocations(self.alloc_cb, self.free_cb, self.realloc_cb)

    def get_code_line(self):
        # first frame is this line, then check_line_changed(), then callback, then actual code.
        st = inspect.stack()[3:]
        for frame in st:
            try:
                filename, line, module, code, index = frame[1:]
                if self.basedir in filename:
                    return frame[1:]
            except:
                pass
        return st[0][1:]

    def alloc_cb(self, ptr, size):
        if size >= self.threshold:
            self.check_line_changed()
            self.blocksizes[ptr] = size
            self.pending_allocations.append(size)

    def free_cb(self, ptr):
        size = self.blocksizes.pop(ptr, 0)
        if size:
            self.check_line_changed()
            self.pending_allocations.append(-size)

    def realloc_cb(self, newptr, oldptr, size):
        if (size >= self.threshold) or (oldptr in self.blocksizes):
            self.check_line_changed()
            oldsize = self.blocksizes.pop(oldptr, 0)
            self.pending_allocations.append(size - oldsize)
            self.blocksizes[newptr] = size

    def check_line_changed(self):
        line = self.get_code_line()
        if line != self.current_line and (self.current_line is not None):
            # move pending events into the allocation_trace
            max_size = self.total_bytes
            bytes_allocated = 0
            bytes_freed = 0
            num_allocations = 0
            num_frees = 0
            before_size = self.total_bytes
            for allocation in self.pending_allocations:
                self.total_bytes += allocation
                if allocation > 0:
                    bytes_allocated += allocation
                    num_allocations += 1
                else:
                    bytes_freed += -allocation
                    num_frees += 1
                max_size = max(max_size, self.total_bytes)
            long_lived = max(self.total_bytes - before_size, 0)
            self.allocation_trace.append((self.current_line, bytes_allocated, bytes_freed, num_allocations, num_frees, max_size, long_lived, time.time()))
            # clear pending allocations
            self.pending_allocations = []
        # move to the new line
        self.current_line = line

    def stop(self):
        self.check_line_changed()  # should force pending to be handled
        np.core.multiarray.trace_data_allocations(None, None, None)

    def write_html(self, filename):
        f = open(filename, "w")
        f.write('<HTML><HEAD><script src="sorttable.js"></script></HEAD><BODY>\n')
        f.write('<TABLE class="sortable" width=100%>\n')
        f.write("<TR>\n")
        cols = "event#,lineinfo,bytes allocated,bytes freed,#allocations,#frees,max memory usage,long lived bytes,time".split(',')
        for header in cols:
            f.write("  <TH>{0}</TH>".format(header))
        f.write("\n</TR>\n")
        prev_time = self.allocation_trace[0][-1]
        for idx, event in enumerate(self.allocation_trace):
            f.write("<TR>\n")
            event = [idx] + list(event)
            for col, val in zip(cols, event):
                if col == 'lineinfo':
                    # special handling
                    try:
                        filename, line, module, code, index = val
                        val = "{0}({1}): {2}".format(filename, line, code[index])
                    except:
                        # sometimes this info is not available (from eval()?)
                        val = str(val)
                elif col == 'time':
                    temp = val
                    val = str(val - prev_time)
                    prev_time = temp
                f.write("  <TD>{0}</TD>".format(val))
            f.write("\n</TR>\n")
        f.write("</TABLE></BODY></HTML>\n")
        f.close()


if __name__ == '__main__':
    tracer = AllocationTracer(1000)
    np.test()
    tracer.start()
    for i in range(100):
        np.zeros(i * 100)
        np.zeros(i * 200)
    np.test()
    tracer.stop()
    tracer.write_html("allocations.html")
