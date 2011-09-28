'''<b>Find Files</b> - find files for processing
See also <b>ExtractMetadata</b>, <b>MakeImageSets</b>.
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
import cellprofiler.cpmodule as cpm
import cellprofiler.settings as cps

from cellprofiler.preferences import \
    DEFAULT_INPUT_FOLDER_NAME, DEFAULT_OUTPUT_FOLDER_NAME, \
    ABSOLUTE_FOLDER_NAME, URL_FOLDER_NAME, \
    DEFAULT_INPUT_SUBFOLDER_NAME, DEFAULT_OUTPUT_SUBFOLDER_NAME, \
    IO_FOLDER_CHOICE_HELP_TEXT


FILES_ALL = "All files in directory"
FILES_SUBSTRING = "Common substring"
FILES_REGEXP = "Regexp"
FILES_ALL_CHOICES = [FILES_ALL, FILES_SUBSTRING, FILES_REGEXP]

class FindFiles(cpm.CPModule):
    module_name = "FindFiles"
    category = 'File Processing'
    variable_revision_number = 1

    def create_settings(self):
        # Location settings
        self.location = cps.DirectoryPath(
            "Input image file location",
            dir_choices=[DEFAULT_INPUT_FOLDER_NAME, DEFAULT_OUTPUT_FOLDER_NAME,
                         ABSOLUTE_FOLDER_NAME, DEFAULT_INPUT_SUBFOLDER_NAME,
                         DEFAULT_OUTPUT_SUBFOLDER_NAME, URL_FOLDER_NAME],
            allow_metadata=False, support_urls=cps.SUPPORT_URLS_SHOW_DIR,
            doc=("Select the folder containing the images to be loaded. " +
                 IO_FOLDER_CHOICE_HELP_TEXT))

        # Matching settings
        self.mode_choice = cps.Choice(
            "Find files by",
            FILES_ALL_CHOICES,
            doc="""Choose the method of finding files (all, substring, regexp)""")

        self.substring = cps.Text(
            "Substring",
            "",
            doc="""What substring do the files have in common?""")

        self.regexp = cps.RegexpText(
            'Regular expression',
            '^(?P<Plate>.*)_(?P<Well>[A-P][0-9]{2})_s(?P<Site>[0-9])',
            get_example_fn=self.example_file_fn,
            doc="""Regular expression for matching files (and optionally extracting metadata).""")

        self.update_list_button = cps.DoSomething(
            "Press to update list",
            "Update...",
            self.update_file_list)

        # XXX - Add multiple matching strategies

        self.file_list = cps.PathFileList(
            'Files found',
            value='',
            doc="""Files matching constraints above.""")

        self.hidden_last_update_settings = cps.Text(
            'Last values for updating',
            value='')

    def settings(self):
        # we include self.file_list, to allow reloading a pipeline and
        # not having to regenerate the list.
        return [self.location, self.mode_choice,
                self.substring, self.regexp,
                self.file_list, self.hidden_last_update_settings]

    def visible_settings(self):
        return ([self.location, self.mode_choice] +
                ([self.substring] if self.mode_choice == FILES_SUBSTRING else []) +
                ([self.regexp] if self.mode_choice == FILES_REGEXP else []) +
                [self.update_list_button, self.file_list])

    def validate_module_warnings(self, pipeline):
        if self.hidden_last_update_settings != self.current_update_settings():
            raise cps.ValidationError("Settings have changed since files were last updated.", self.file_list)

    def current_update_settings(self):
        return str((self.location.get_absolute_path(), self.mode_choice,
                    ([self.substring] if self.mode_choice == FILES_SUBSTRING else []) +
                    ([self.regexp] if self.mode_choice == FILES_REGEXP else [])))

    def update_file_list(self):
        self.file_list.value = self.collect_files()
        self.hidden_last_update_settings = self.current_update_settings()

    def run(self, workspace):
        pass

    def example_file_fn(self, path=None):
        # stolen from LoadImages, XXX - should be combined into one utility function
        '''Get an example file for use in the file metadata regexp editor'''
        if path is None:
            path = self.location.get_absolute_path()
            default = "plateA-2008-08-06_A12_s1_w1_[89A882DE-E675-4C12-9F8E-46C9976C4ABE].tif"
        else:
            default = None

        filenames = [x for x in os.listdir(path)
                     if os.path.splitext(x)[1].upper() in ('.TIF', '.JPG', '.PNG', '.BMP')]
        if len(filenames) > 0:
            return filenames[0]
        return default

    def collect_files(self):
        """Collect the files that match the filter criteria

        Returns a list of tuples, each with (Path, Filename, Metadata).

        Metadata is a dict, present in the case of named groups if
        using regular expressions, as well as implicit values from
        indexing.  XXX - add indexing info?  XXX - add subdirs
        """
        root = self.location.get_absolute_path()
        # can be overridden for URLs
        listdir = lambda path: [x for x in os.listdir(path)
                                if os.path.isfile(os.path.join(path, x))]
        if (root.lower().startswith("http:") or
            root.lower().startswith("https:")):
            from cellprofiler.utilities.read_directory_url \
                 import walk_url, read_directory_url, IS_FILE
            listdir = lambda path: [x for x, y in read_directory_url(path)
                                    if y == IS_FILE]

        files = [(root, file_name) for file_name in listdir(root)]

        if self.mode_choice == FILES_REGEXP:
            # filter_regexp() will return extra metadata as a dict if it's available
            files = [self.filter_regexp(path, file_name)
                     for path, file_name in files]
        elif self.mode_choice == FILES_SUBSTRING:
            # no metadata
            files = [(path, file_name, {})
                     for path, file_name in files
                     if self.substring.value in file_name]
        else:
            assert self.mode_choice == FILES_ALL
            # no metadata
            files = [(path, file_name, {}) for path, file_name in files]

        return sorted(files)

    def upgrade_settings(self, setting_values, variable_revision_number,
                         module_name, from_matlab):
        assert not from_matlab
        return setting_values, variable_revision_number, from_matlab
