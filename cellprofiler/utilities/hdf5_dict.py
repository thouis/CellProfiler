"""hdf5_dict -- HDF5-backed dictionary for Measurements.

This module implements the HDF5Dict class, which provides a dict-like
interface for measurements, backed by an HDF5 file.

CellProfiler is distributed under the GNU General Public License,
but this file is licensed under the more permissive BSD license.
See the accompanying file LICENSE for details.

Copyright (c) 2003-2009 Massachusetts Institute of Technology
Copyright (c) 2009-2012 Broad Institute
Copyright (c) 2011 Institut Curie
All rights reserved.

Please see the AUTHORS file for credits.

Website: http://www.cellprofiler.org
"""
from __future__ import with_statement

__version__ = "$Revision$"

import os
import threading
import numpy as np
import h5py
import time
import logging
logger = logging.getLogger(__name__)

version_number = 2
VERSION = "Version"

# h5py is nice, but not being able to make zero-length selections is a pain.
orig_hdf5_getitem = h5py.Dataset.__getitem__
def new_getitem(self, args):
    if (isinstance(args, slice) and \
            args.start is not None and args.start == args.stop):
        return np.array([], self.dtype)
    return orig_hdf5_getitem(self, args)
setattr(h5py.Dataset, orig_hdf5_getitem.__name__, new_getitem)

orig_hdf5_setitem = h5py.Dataset.__setitem__
def new_setitem(self, args, val):
    if isinstance(args, slice) and \
            args.start is not None and args.start == args.stop:
        return np.array([], self.dtype)[0:0]
    return orig_hdf5_setitem(self, args, val)
setattr(h5py.Dataset, orig_hdf5_setitem.__name__, new_setitem)

def infer_hdf5_type(val):
    val = np.asanyarray(val)
    if val.dtype.kind in 'SUa':
        return np.uint8
    if val.size == 0:
        return int
    return np.asanyarray(val).dtype

def unwrap_strings(data):
    if data.dtype == np.uint8:  # unsigned int8
        return data.view(('U', data.size / 4))
    return data


class HDF5Dict(object):
    '''The HDF5Dict can be used to store data indexed by a tuple of
    two strings and a non-negative integer.

    measurements = HDF5Dict(hdf5_filename)

    # Experiment-level features
    measurements['Experiment', 'feature1', 0] = 'a'
    measurements['Experiment', 'feature2', 0] = 1

    # Image-level features
    measurements['Image', 'imfeature1', 1] = 'foo'
    measurements['Image', 'imfeature2', 1] = 5

    # Object-level features
    measurements['Object1', 'objfeature1', 1] = [1, 2, 3]
    measurements['Object1', 'objfeature2', 1] = [4.0, 5.0, 6.0]

    Note that fetch operations always return either a single value or
    1D array, depending on what was stored.  The last integer can be
    any non-negative value, and does not need to be assigned in order.

    Slicing is not allowed in assignment or fetching.

    Integers, floats, and strings can be stored in the measurments.
    Strings will be returned as utf-8.

    Data can be removed with the del operator.

    del measurements['Experiment', 'feature1', 0]  # ok
    del measurements['Image', 'imfeature1', 2]  # ok

    If the 'must_exist' flag is set, it is an error to add a new
    object or feature that does not exist.
    
    The measurements data is stored in groups corresponding to object names
    (with special objects, "Image" = image set measurements and "Experiment" = 
    experiment measurements. Each object feature has its own group under
    the object group. The feature group has two data sets. The first data set
    is "index" and holds indexes into the second data set whose name is "data".
    "index" is an N x 3 integer array where N is the number of image sets
    with this feature measurement and the three row values are the image number
    of that row's measurements, the offset to the first data element for
    the feature measurement for that image number in the "data" dataset 
    and the offset to one past the last data element.
    '''

    # XXX - document how data is stored in hdf5 (basically, /Measurements/Object/Feature)

    def __init__(self, hdf5_filename, 
                 top_level_group_name = "Measurements",
                 run_group_name = time.strftime("%Y-%m-%d-%H-%m-%S"),
                 is_temporary = False,
                 copy = None):
        self.is_temporary = is_temporary
        self.filename = hdf5_filename
        logger.debug("HDF5Dict.__init__(): %s, temporary=%s, copy=%s", self.filename, self.is_temporary, copy)
        # assert not os.path.exists(self.filename)  # currently, don't allow overwrite
        self.hdf5_file = h5py.File(self.filename, 'w')
        vdataset = self.hdf5_file.create_dataset(
            VERSION, data = np.array([version_number], int))
        self.top_level_group_name = top_level_group_name
        mgroup = self.hdf5_file.create_group(top_level_group_name)
        self.top_group = mgroup.create_group(run_group_name)
        self.indices = {}  # nested indices for data slices, indexed by (object, feature) then by numerical index
        class HDF5Lock:
            def __init__(self):
                self.lock = threading.RLock()
            def __enter__(self):
                self.lock.acquire()
                if hasattr(h5py.highlevel, "phil"):
                    h5py.highlevel.phil.acquire()
                
            def __exit__(self, t, v, tb):
                if hasattr(h5py.highlevel, "phil"):
                    h5py.highlevel.phil.release()
                self.lock.release()
                
        self.lock = HDF5Lock()
                
        self.must_exist = False
        self.chunksize = 1024
        if copy is not None:
            for object_name in copy.keys():
                object_group = copy[object_name]
                self.top_group.copy(object_group, self.top_group)
                for feature_name in object_group.keys():
                    # some measurement objects are written at a higher level, and don't
                    # have an index (e.g. Relationship).
                    if 'index' in object_group[feature_name].keys():
                        d = self.indices[object_name, feature_name] = {}
                        hdf5_index = object_group[feature_name]['index'][:]
                        for num_idx, start, stop in hdf5_index:
                            d[num_idx] = slice(start, stop)
            self.hdf5_file.flush()

    def __del__(self):
        if logger:  # avoid spurious errors on script shutdown
            logger.debug("HDF5Dict.__del__(): %s, temporary=%s", self.filename, self.is_temporary)
        self.close()
        
    def close(self):
        if not hasattr(self, "hdf5_file"):
            # This happens if the constructor could not open the hdf5 file
            return
        if self.is_temporary:
            try:
                self.hdf5_file.flush()  # just in case unlink fails
                self.hdf5_file.close()
                os.unlink(self.filename)
            except Exception, e:
                logger.warn("So sorry. CellProfiler failed to remove the temporary file, %s and there it sits on your disk now." % self.filename)
        else:
            self.hdf5_file.flush()
            self.hdf5_file.close()
        del self.hdf5_file

    def flush(self):
        logger.debug("HDF5Dict.flush(): %s, temporary=%s", self.filename, self.is_temporary)
        self.hdf5_file.flush()

    def __getitem__(self, idxs):
        assert isinstance(idxs, tuple), "Accessing HDF5_Dict requires a tuple of (object_name, feature_name[, integer])"
        assert isinstance(idxs[0], basestring) and isinstance(idxs[1], basestring), "First two indices must be of type str."
        assert ((not np.isscalar(idxs[2]) and np.all(idxs[2] >= 0))
                or (isinstance(idxs[2], int) and idxs[2] >= 0)),\
               "Third index must be a non-negative integer or integer array"

        object_name, feature_name, num_idx = idxs
        feature_exists = self.has_feature(object_name, feature_name)
        assert feature_exists

        if not np.isscalar(num_idx):
            with self.lock:
                indices = self.indices[(object_name, feature_name)]
                dataset = self.get_dataset(object_name, feature_name)
                return [None if (isinstance(src, slice) and
                                 src.start is not None and
                                 src.start == src.stop) else unwrap_strings(dataset[src])
                        for src in [indices.get(image_number, slice(0, 0))
                                    for image_number in num_idx]]

        if not self.has_data(*idxs):
            return None

        with self.lock:
            src = self.find_index_or_slice(idxs)
            dataset = self.get_dataset(object_name, feature_name)
            return unwrap_strings(dataset[src])

    def __setitem__(self, idxs, val):
        assert isinstance(idxs, tuple), "Assigning to HDF5_Dict requires a tuple of (object_name, feature_name, integer)"
        assert isinstance(idxs[0], basestring) and isinstance(idxs[1], basestring), "First two indices must be of type str."
        assert isinstance(idxs[2], int) and idxs[2] >= 0, "Third index must be a non-negative integer"

        object_name, feature_name, num_idx = idxs
        full_name = '%s.%s' % (idxs[0], idxs[1])
        feature_exists = self.has_feature(object_name, feature_name)
        assert (not self.must_exist) or feature_exists, \
            "Attempted storing new feature %s, but must_exist=True" % (full_name)

        if not feature_exists:
            if not self.has_object(object_name):
                self.add_object(object_name)
            self.add_feature(object_name, feature_name)

        # find the destination for the data, and check that its
        # the right size for the values.  This may extend the
        # _index and data arrays. It may also overwrite the old value.
        dest = self.find_index_or_slice(idxs, val)

        with self.lock:
            dataset = self.get_dataset(object_name, feature_name)

            # If we store an integer, then later a float, we need to promote here.
            if dataset.dtype.kind == 'i':
                if np.asanyarray(val).dtype.kind == 'f':
                    # it's possible we have only stored integers and now need to promote to float
                    if dataset.shape[0] > 0:
                        vals = dataset[:].astype(float)
                    else:
                        vals = np.array([])
                    del self.top_group[object_name][feature_name]['data']
                    dataset = self.top_group[object_name][feature_name].create_dataset('data', (vals.size,), dtype=float,
                                                                                       compression='gzip', shuffle=True, chunks=(self.chunksize,), maxshape=(None,))
                    if vals.size > 0:
                        dataset[:] = vals
                elif np.asanyarray(val).dtype.kind in 'SUa':
                    # we created the dataset without any data, so didn't know the type before.  We defaulted to int, and now need to become a string.
                    sz = dataset.shape[0]
                    assert sz == 0, "Trying to write a string to an integer-typed array"
                    del self.top_group[object_name][feature_name]['data']
                    dataset = self.top_group[object_name][feature_name].create_dataset('data', (sz,), dtype=np.uint8,
                                                                                       compression='gzip', shuffle=True, chunks=(self.chunksize,), maxshape=(None,))
            val = np.asanyarray(val)
            if (val.dtype.kind in 'SUa') or (dataset.dtype == np.uint8):
                if dataset.dtype == np.uint8:
                    dataset[dest] = val.astype('U').flatten().view(np.uint8)
                else:
                    dataset[dest] = val  # old style HDF5
            elif np.isscalar(val):
                dataset[dest] = val
            else:
                dataset[dest] = np.asanyarray(val).ravel()

    def __delitem__(self, idxs):
        assert isinstance(idxs, tuple), "Accessing HDF5_Dict requires a tuple of (object_name, feature_name, integer)"
        assert isinstance(idxs[0], basestring) and isinstance(idxs[1], basestring), "First two indices must be of type str."
        assert isinstance(idxs[2], int) and idxs[2] >= 0, "Third index must be a non-negative integer"

        object_name, feature_name, num_idx = idxs
        feature_exists = self.has_feature(object_name, feature_name)
        assert feature_exists

        if not self.has_data(*idxs):
            return

        with self.lock:
            dest = self.find_index_or_slice(idxs)
            # it's possible we're fetching data from an image without
            # any objects, in which case we probably weren't able to
            # infer a type in __setitem__(), which means there may be
            # no dataset, yet.
            del self.indices[object_name, feature_name][num_idx]
            # reserved value of -1 means deleted
            idx = self.top_group[object_name][feature_name]['index']
            idx[np.flatnonzero(idx[:, 0] == num_idx), 0] = -1
            
    def has_data(self, object_name, feature_name, num_idx):
        return num_idx in self.indices.get((object_name, feature_name), [])

    def get_dataset(self, object_name, feature_name):
        with self.lock:
            return self.top_group[object_name][feature_name]['data']

    def has_object(self, object_name):
        with self.lock:
            return object_name in self.top_group

    def add_object(self, object_name):
        with self.lock:
            object_group = self.top_group.require_group(object_name)

    def has_feature(self, object_name, feature_name):
        return (object_name, feature_name) in self.indices

    def add_feature(self, object_name, feature_name):
        with self.lock:
            feature_group = self.top_group[object_name].require_group(feature_name)
            self.indices.setdefault((object_name, feature_name), {})
            
    def find_index_or_slice(self, idxs, values=None):
        '''Find the linear indexes or slice for a particular set of
        indexes "idxs", and check that values could be stored in that
        linear index or slice.  If the linear index does not exist for
        the given idxs, then it will be created with sufficient size
        to store values (which must not be None, in this case).  If
        the dataset does not exist, it will be created by this method.
        '''
        with self.lock:
            object_name, feature_name, num_idx = idxs
            assert isinstance(num_idx, int)
            index = self.indices[object_name, feature_name]
            if (num_idx not in index) and (values is None):
                return None  # no data
            if values is not None:
                values = np.asanyarray(values)
                feature_group = self.top_group.require_group(object_name).require_group(feature_name)
                if (values.dtype.kind in 'SUa'):
                    if ('data' in feature_group) and (feature_group['data'].dtype == h5py.special_dtype(vlen=str)):
                        # old-style HDF5Ddict: store strings as vlen strings
                        data_size = values.ravel().size
                    else:
                        # store as UTF-8 in uint8s
                        data_size = values.astype('U').nbytes
                else:
                    data_size = values.ravel().size
                if num_idx in index:
                    sl = index[num_idx]
                    if data_size > (sl.stop - sl.start):
                        hdf5_index = feature_group['index']
                        hdf5_index[np.flatnonzero(hdf5_index[:, 0] == num_idx), 0] = -1
                        del index[num_idx]
                    elif data_size < (sl.stop - sl.start):
                        hdf5_index = feature_group['index']
                        loc = np.flatnonzero(hdf5_index[:, 0] == num_idx)
                        hdf5_index[loc, 2] = hdf5_index[loc, 1] + data_size
                        index[num_idx] = slice(sl.start, sl.start + data_size)
                if num_idx not in index:
                    grow_by = data_size
                    # create the measurements if needed
                    if not 'data' in feature_group:
                        feature_group.create_dataset('data', (0,), dtype=infer_hdf5_type(values),
                                                     compression='gzip', shuffle=True, chunks=(self.chunksize,), maxshape=(None,))
                        feature_group.create_dataset('index', (0, 3), dtype=int, compression=None,
                                                     chunks=(self.chunksize, 3), maxshape=(None, 3))
                    # grow data and index
                    ds = feature_group['data']
                    hdf5_index = feature_group['index']
                    cur_size = ds.shape[0]
                    ds.resize((cur_size + grow_by,))
                    hdf5_index.resize((hdf5_index.shape[0] + 1, 3))
                    # store locations for new data
                    hdf5_index[-1, :] = (num_idx, cur_size, cur_size + grow_by)
                    index[num_idx] = slice(cur_size, cur_size + grow_by)
            return index[num_idx]

    def clear(self):
        with self.lock:
            del self.hdf5_file[self.top_level_group_name]
            self.top_group = self.hdf5_file.create_group(self.top_level_group_name)
            self.indices = {}

    def erase(self, object_name, first_idx, mask):
        with self.lock:
            self.top_group[object_name]['_index'][mask] = -1
            self.level1_indices[object_name].pop(first_idx, None)

    def get_indices(self, object_name, feature_name):
        # CellProfiler expects these in write order
        if not (self.has_object(object_name) and 
                self.has_feature(object_name, feature_name)):
            return []
        with self.lock:
            if 'index' in self.top_group[object_name][feature_name]:
                idxs = self.top_group[object_name][feature_name]['index'][:, 0][:]
                return idxs[idxs != -1]
            else:
                return []

    def top_level_names(self):
        with self.lock:
            return self.top_group.keys()

    def second_level_names(self, object_name):
        with self.lock:
            return self.top_group[object_name].keys()
        
    def add_all(self, object_name, feature_name, values, idxs = None):
        '''Add all imageset values for a given feature
        
        object_name - name of object supporting the feature
        feature_name - name of the feature
        values - either a list of scalar values or a list of arrays
                 where each array has the values for each of the
                 objects in the corresponding image set.
        idxs - the image set numbers associated with the values. If idxs is
               omitted or None, image set numbers are assumed to go from 1 to N
        '''
        with self.lock:
            self.add_object(object_name)
            if self.has_feature(object_name, feature_name):
                del self.top_group[object_name][feature_name]
                del self.indices[object_name, feature_name]
            self.add_feature(object_name, feature_name)
            if idxs is None:
                idxs = [i+1 for i, value in enumerate(values)
                        if value is not None]
                values = [value for value in values if value is not None]
            if len(values) > 0:
                if np.isscalar(values[0]):
                    idx = np.column_stack((idxs,
                                           np.arange(len(idxs)),
                                           np.arange(len(idxs))+1))
                    assert not isinstance(values[0], unicode), "Unicode must be string encoded prior to call"
                    if isinstance(values[0], str):
                        dataset = np.array(
                            [value for value in values if value is not None],
                            object)
                        dtype = h5py.special_dtype(vlen=str)
                    else:
                        dataset = np.array(values)
                        dtype = dataset.dtype
                else:
                    counts = np.array([len(x) for x in values])
                    offsets = np.hstack([[0], np.cumsum(counts)])
                    idx = np.column_stack((idxs, offsets[:-1], offsets[1:]))
                    dataset = np.hstack(values)
                    dtype = dataset.dtype
                
                self.indices[object_name, feature_name] = dict([
                    (i, slice(start, end)) 
                    for i, start, end in idx])
                feature_group = self.top_group[object_name][feature_name]
                feature_group.create_dataset(
                    'data', data = dataset, 
                    dtype = dtype, compression = 'gzip', shuffle=True,
                    chunks = (self.chunksize, ), 
                    maxshape = (None, ))
                feature_group.create_dataset(
                    'index', data = idx, dtype=int,
                    compression = None, chunks = (self.chunksize, 3),
                    maxshape = (None,3))

def get_top_level_group(filename, group_name = 'Measurements', open_mode='r'):
    '''Open and return the Measurements HDF5 group
    
    filename - path to HDF5 file
    
    group_name - name of top-level group, defaults to Measurements group
    
    open_mode - open mode for file: 'r' for read, 'w' for write
    
    returns the hdf5 file object (which must be closed) and the top-level group
    '''
    f = h5py.File(filename, open_mode)
    return f, f.get(group_name)

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    h = HDF5Dict('temp.hdf5')
    h['Object1', 'objfeature1', 1] = 'sdf'
    h['Image', 'f3', 1] = 'foo'
    h['Image', 'f3', 2] = 'foo2'
    print h['Image', 'f3', 1]
    print h['Image', 'f3', [1,2]]
    h['Image', 'f1', 2] = 6
    h['Image', 'f2', 1] = 6
    print h['Image', 'f2', 1]
    print h['Object1', 'objfeature1', 1]
    h['Object1', 'objfeature1', 2] = 3.0
    print h['Object1', 'objfeature1', 2]
    h['Object1', 'objfeature1', 1] = [1, 2, 3]
    h['Object1', 'objfeature1', 1] = [1, 2, 3, 5, 6]
    h['Object1', 'objfeature1', 1] = [9, 4.0, 2.5]
    print     h['Object1', 'objfeature1', 1]
