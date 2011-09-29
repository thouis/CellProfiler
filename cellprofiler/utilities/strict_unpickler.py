##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
# Modified 2011-9-29 by Thouis Jones (changed name, added more opcodes)
##############################################################################

import pickle
import logging
import pickletools
from cStringIO import StringIO

if pickle.format_version != "2.0":
    # Maybe the format changed, and opened a security hole
    raise NotImplementedError('Invalid pickle version for StrictUnpickler')

class StrictUnpickler(pickle.Unpickler):
    """An unpickler that can only handle simple types.
    """
    def refuse_to_unpickle(self):
        raise pickle.UnpicklingError, 'Refused'

    dispatch = pickle.Unpickler.dispatch.copy()

    for k, v in sorted(dispatch.items()):
        if k == '' or k in '().012FGIJKLMNTUVXS]adeghjlpqrstu}' or \
                k in [pickle.PROTO, pickle.TUPLE1, pickle.TUPLE2, pickle.TUPLE3,
                      pickle.NEWTRUE, pickle.NEWFALSE, pickle.LONG1, pickle.LONG4]:
            # This key is necessary and safe, so leave it in the map
            pass
        else:
            dispatch[k] = refuse_to_unpickle
            # Anything unnecessary is banned, but here is some logic to explain why
            if k in [pickle.GLOBAL, pickle.OBJ, pickle.INST, pickle.REDUCE, pickle.BUILD,
                     pickle.NEWOBJ, pickle.EXT1, pickle.EXT2, pickle.EXT4]:
                # These are definite security holes
                pass
            elif k in [pickle.PERSID, pickle.BINPERSID]:
                # These are just unnecessary
                pass
            else:
                logging.warning("UNEXPLAINED OPCODE %d %s", ord(k), pickletools.code2op[k].name)
    del k
    del v

def _should_succeed(x, binary=1):
    if x != StrictUnpickler(StringIO(pickle.dumps(x, binary))).load():
        raise ValueError(x)

def _should_fail(x, binary=1):
    try:
        StrictUnpickler(StringIO(pickle.dumps(x, binary))).load()
        raise ValueError(x)
    except pickle.UnpicklingError, e:
        if e[0] != 'Refused': raise ValueError(x)

class _junk_class:
    pass

def _test():
    _should_succeed('hello')
    _should_succeed(1)
    _should_succeed(1L)
    _should_succeed(1.0)
    _should_succeed((1, 2, 3))
    _should_succeed([1, 2, 3])
    _should_succeed({1 : 2, 3 : 4})
    _should_fail(open)
    _should_fail(_junk_class)
    _should_fail(_junk_class())

# Test MiniPickle on every import
_test()
