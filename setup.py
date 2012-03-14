"""
CellProfiler is distributed under the GNU General Public License.
See the accompanying file LICENSE for details.

Copyright (c) 2003-2009 Massachusetts Institute of Technology
Copyright (c) 2009-2010 Broad Institute
All rights reserved.

Please see the AUTHORS file for credits.

Website: http://www.cellprofiler.org
"""
__version__ = "$Revision$"

from setuptools import setup
import sys
import os
import os.path
import glob
from subprocess import call


# fix from
#  http://mail.python.org/pipermail/pythonmac-sig/2008-June/020111.html
import pytz
pytz.zoneinfo = pytz.tzinfo
pytz.zoneinfo.UTC = pytz.UTC

from libtiff.libtiff_ctypes import tiff_h_name

if sys.platform == "darwin":
    os.system("svn info | grep Revision | sed -e 's/Revision:/\"Version/' -e 's/^/VERSION = /' -e 's/$/\"/' > version.py")

# We get libfreetype and libpng from /usr/X11R6/lib
temp = os.environ.get('DYLD_LIBRARY_PATH', None)
os.environ['DYLD_LIBRARY_PATH'] = temp + ':/usr/X11/lib' if temp else '/usr/X11/lib'

APPNAME = 'CellProfiler2.0'
APP = ['CellProfiler.py']
DATA_FILES = [('cellprofiler/icons', glob.glob(os.path.join('.', 'cellprofiler', 'icons', '*.png'))),
              ('bioformats', ['bioformats/loci_tools.jar']),
              ('imagej', ['imagej/TCPClient.class', 'imagej/InterProcessIJBridge.class',
                          'imagej/InterProcessIJBridge$1.class', 'imagej/ij.jar']),
              ]
OPTIONS = {'argv_emulation': True,
           'packages': ['cellprofiler', 'contrib', 'bioformats', 'imagej'],
           'includes': ['numpy', 'wx', 'matplotlib','email.iterators', 'smtplib',
                        'sqlite3', 'libtiff', 'wx.lib.intctrl', 'libtiff.'+tiff_h_name,
                        'xml.dom.minidom'],
           'excludes': ['pylab', 'nose', 'Tkinter', 'Cython', 'scipy.weave'],
           'resources': ['CellProfilerIcon.png', 'cellprofiler/icons'],
           'iconfile' : 'CellProfilerIcon.icns',
           'frameworks' : ['libtiff.dylib']
           }

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
    name="CellProfiler2.0"
)

if sys.argv[-1] == 'py2app':
    # there should be some way to do this within setup's framework, but I don't
    # want to figure it out right now, and our setup is going to be changing
    # significantly soon, anyway.
    call('find dist/CellProfiler2.0.app -name tests -type d | xargs rm -rf', shell=True)
    call('lipo dist/CellProfiler2.0.app/Contents/MacOS/CellProfiler2.0 -thin i386 -output dist/CellProfiler2.0.app/Contents/MacOS/CellProfiler2.0', shell=True)
    call('rm dist/CellProfiler2.0.app/Contents/Resources/lib/python2.7/cellprofiler/icons/*.png', shell=True)
    call('cp /usr/X11/lib/libfreetype.6.dylib dist/CellProfiler2.0.app/Contents/Frameworks', shell=True)
    call('cp /usr/X11/lib/libpng12.0.dylib dist/CellProfiler2.0.app/Contents/Frameworks', shell=True)
    call('find dist/CellProfiler2.0.app -name \*.so -o -name \*.dylib | xargs -n 1 install_name_tool -change "/usr/X11/lib/libfreetype.6.dylib" "@executable_path/../Frameworks/libfreetype.6.dylib"', shell=True)
    call('find dist/CellProfiler2.0.app -name \*.so -o -name \*.dylib | xargs -n 1 install_name_tool -change "/usr/X11/lib/libpng12.0.dylib" "@executable_path/../Frameworks/libpng12.0.dylib"', shell=True)
