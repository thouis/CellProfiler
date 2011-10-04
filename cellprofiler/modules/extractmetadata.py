'''<b>Extract Metadata</b> - Extract Metadata from filenames
See also <b>FindFiles</b>, <b>MakeImageSets</b>.
'''
# CellProfiler is distributed under the GNU General Public License.
# See the accompanying file LICENSE for details.
#
# Developed by the Broad Institute
# Copyright 2003-2011
#
# Please see the AUTHORS file for credits.
#
# Website: http://www.cellprofiler.org

__version__ = "$Revision$"

import os
import os.path
import sys
import re
import cellprofiler.cpmodule as cpm
import cellprofiler.settings as cps

class ExtractMetadata(cpm.CPModule):
    module_name = "ExtractMetadata"
    category = 'File Processing'
    variable_revision_number = 1

    def create_settings(self):
        self.regexp = cps.RegexpText(
            'Regular expression',
            '^(?P<Plate>.*)_(?P<Well>[A-P][0-9]{2})_s(?P<Site>[0-9])',
            get_example_fn=self.example_file_fn,
            doc="""Regular expression for extracting metadata.""")

        # XXX - Add multiple matching strategies

        self.file_metadata_list = cps.DictList(
            'Files and metadata',
            value=[],
            primary_keys=['URL'],
            doc="""Files and metadata.""")

        self.last_updated_from = None
        self.last_found_findfiles = None

    def settings(self):
        return [self.regexp]

    def visible_settings(self):
        return [self.regexp, self.file_metadata_list]

    def validate_module_warnings(self, pipeline):
        previous_find_files = self.find_findfiles_module(pipeline)
        if not previous_find_files:
            raise cps.ValidationError("ExtractMetadata needs a FindFiles module.", self.regexp)
        # XXX - this should not be automatic, for reasons of speed
        # and, possibly, thread safety.  But it might be hooked up to
        # updates of FindFiles's list, somehow.
        if self.last_updated_from != (self.regexp.value, previous_find_files.file_list.value):
            try:
                self.file_metadata_list.value = self.update(previous_find_files.file_list.value)
                self.last_updated_from = (self.regexp.value, previous_find_files.file_list.value)
            except Exception, e:
                import traceback
                raise cps.ValidationError("Exception extracting metadata: %s\n%s" % (e, "\n".join(traceback.format_exception(*sys.exc_info()))), self.regexp)

    def find_findfiles_module(self, pipeline):
        result = None
        for m in pipeline.modules():
            if m.module_name == 'FindFiles':
                result = m
            if m == self:
                break
        self.last_found_findfiles = result
        return result

    def post_pipeline_load(self, pipeline):
        self.find_findfiles_module(pipeline)

    def update(self, file_list):
        pattern = re.compile(self.regexp.value)
        def genlist():
            for filedict in file_list:
                m = pattern.search(filedict['URL'])
                if m is not None:
                    d = m.groupdict()
                    d.update(filedict)
                    yield d
                else:
                    yield filedict
        new_list = [v for v in genlist()]
        return new_list

    def run(self, workspace):
        pass

    def example_file_fn(self, path=None):
        '''Get an example file for use in the file metadata regexp editor'''
        previous_find_files = self.last_found_findfiles
        try:
            return os.path.join(*(previous_find_files.file_list.value[0][:2]))
        except Exception, e:
            return "plateA-2008-08-06_A12_s1_w1_[89A882DE-E675-4C12-9F8E-46C9976C4ABE].tif"

    def upgrade_settings(self, setting_values, variable_revision_number,
                         module_name, from_matlab):
        assert not from_matlab
        return setting_values, variable_revision_number, from_matlab
