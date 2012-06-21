'''<b>Metadata</b> - Associate metadata with images
<hr>
TO-DO: document module
'''
import logging
logger = logging.getLogger(__name__)
import csv
import re
import os

import cellprofiler.cpmodule as cpm
import cellprofiler.measurements as cpmeas
import cellprofiler.pipeline as cpp
import cellprofiler.settings as cps
from cellprofiler.modules.images import FilePredicate
from cellprofiler.modules.images import ExtensionPredicate
from cellprofiler.modules.images import ImagePredicate
from cellprofiler.modules.images import DirectoryPredicate
from cellprofiler.modules.images import Images

X_AUTOMATIC_EXTRACTION = "Automatic"
X_MANUAL_EXTRACTION = "Manual"
X_IMPORTED_EXTRACTION = "Import metadata"
X_ALL_EXTRACTION_METHODS = [X_AUTOMATIC_EXTRACTION, 
                            X_MANUAL_EXTRACTION,
                            X_IMPORTED_EXTRACTION]
XM_FILE_NAME = "From file name"
XM_FOLDER_NAME = "From folder name"

F_ALL_IMAGES = "All images"
F_FILTERED_IMAGES = "Images selected using a filter"
COL_PATH = "Path / URL"
COL_SERIES = "Series"
COL_INDEX = "Index"
COL_CHANNEL = "Channel"

'''Index of the extraction method count in the settings'''
IDX_EXTRACTION_METHOD_COUNT = 1
'''Index of the first extraction method block in the settings'''
IDX_EXTRACTION_METHOD = 2
'''# of settings in an extraction method block'''
LEN_EXTRACTION_METHOD = 6

class Metadata(cpm.CPModule):
    variable_revision_number = 1
    module_name = "Metadata"
    category = "File Processing"
    
    CSV_JOIN_NAME = "CSV Metadata"
    IPD_JOIN_NAME = "Image Metadata"

    def create_settings(self):
        self.pipeline = None
        self.ipds = []
        self.imported_metadata = []
        self.metadata_explanation = cps.HTMLText(
            "","""Do your file or path names or file headers contain information
            (<a href="http://en.wikipedia.org/wiki/Metadata">metadata</a>) you
            would like to extract and store along with your measurements?""",
            size=(0, 2.3))
        self.wants_metadata = cps.Binary(
            "Extract metadata?", False,
        doc = "Do your file or path names or file headers contain information\n"
            "(metadata) you would like to extract and store along with your "
            "measurements?")
        
        self.extraction_methods = []
        self.add_extraction_method(False)
        self.extraction_method_count = cps.HiddenCount(
            self.extraction_methods, "Extraction method count")
        self.add_extraction_method_button = cps.DoSomething(
            "Add another extraction method",
            "Add", self.add_extraction_method)
        self.table = cps.Table("")
        self.update_table_button = cps.DoSomething(
            "Update table", "Update", self.update_table)
        
    def add_extraction_method(self, can_remove = True):
        group = cps.SettingsGroup()
        self.extraction_methods.append(group)
        if can_remove:
            group.append("divider", cps.Divider())
            
        group.append("extraction_method", cps.Choice(
            "Extraction method", X_ALL_EXTRACTION_METHODS,
            doc="To do"))
        
        group.append("source", cps.Choice(
            "Source", [XM_FILE_NAME, XM_FOLDER_NAME],
            doc = """Do you want to extract metadata from the image's file
            name or from its folder name?"""))
        
        group.append("file_regexp", cps.RegexpText(
            "Regular expression", 
            '^(?P<Plate>.*)_(?P<Well>[A-P][0-9]{2})_s(?P<Site>[0-9])_w(?P<ChannelNumber>[0-9])',
            get_example_fn = self.example_file_fn,
            doc = """
            <a name='regular_expression'><i>(Used only if you want to extract 
            metadata from the file name)</i><br>
            The regular expression to extract the metadata from the file name 
            is entered here. Note that this field is available whether you have 
            selected <i>Text-Regular expressions</i> to load the files or not.
            Please see the general module help for more information on 
            construction of a regular expression.</a>
            <p>Clicking the magnifying glass icon to the right will bring up a
            tool for checking the accuracy of your regular expression. The 
            regular expression syntax can be used to name different parts of 
            your expression. The syntax <i>(?P&lt;fieldname&gt;expr)</i> will 
            extract whatever matches <i>expr</i> and assign it to the 
            measurement,<i>fieldname</i> for the image.
            <p>For instance, a researcher uses plate names composed of a string 
            of letters and numbers, followed by an underscore, then the well, 
            followed by another underscore, followed by an "s" and a digit
            representing the site taken within the well (e.g., <i>TE12345_A05_s1.tif</i>).
            The following regular expression will capture the plate, well, and 
            site in the fields "Plate", "Well", and "Site":<br><br>
            <table border = "1">
            <tr><td colspan = "2">^(?P&lt;Plate&gt;.*)_(?P&lt;Well&gt;[A-P][0-9]{1,2})_s(?P&lt;Site&gt;[0-9])</td></tr>
            <tr><td>^</td><td>Start only at beginning of the file name</td></tr>
            <tr><td>(?P&lt;Plate&gt;</td><td>Name the captured field <i>Plate</i></td></tr>
            <tr><td>.*</td><td>Capture as many characters as follow</td></tr>
            <tr><td>_</td><td>Discard the underbar separating plate from well</td></tr>
            <tr><td>(?P&lt;Well&gt;</td><td>Name the captured field <i>Well</i></td></tr>
            <tr><td>[A-P]</td><td>Capture exactly one letter between A and P</td></tr>
            <tr><td>[0-9]{1,2}</td><td>Capture one or two digits that follow</td></tr>
            <tr><td>_s</td><td>Discard the underbar followed by <i>s</i> separating well from site</td></tr>
            <tr><td>(?P&lt;Site&gt;</td><td>Name the captured field <i>Site</i></td></tr>
            <tr><td>[0-9]</td><td>Capture one digit following</td></tr>
            </table>
            
            <p>The regular expression can be typed in the upper text box, with 
            a sample file name given in the lower text box. Provided the syntax 
            is correct, the corresponding fields will be highlighted in the same
            color in the two boxes. Press <i>Submit</i> to enter the typed 
            regular expression.</p>
            
            <p>You can create metadata tags for any portion of the filename or path, but if you are
            specifying metadata for multiple images in a single <b>LoadImages</b> module, an image cycle can 
            only have one set of values for each metadata tag. This means that you can only 
            specify the metadata tags which have the same value across all images listed in the module. For example,
            in the example above, you might load two wavelengths of data, one named <i>TE12345_A05_s1_w1.tif</i>
            and the other <i>TE12345_A05_s1_w2.tif</i>, where the number following the <i>w</i> is the wavelength. 
            In this case, a "Wavelength" tag <i>should not</i> be included in the regular expression
            because while the "Plate", "Well" and "Site" metadata is identical for both images, the wavelength metadata is not.</p>
            
            <p>Note that if you use the special fieldnames <i>&lt;WellColumn&gt;</i> and 
            <i>&lt;WellRow&gt;</i> together, LoadImages will automatically create a <i>&lt;Well&gt;</i>
            metadata field by joining the two fieldname values together. For example, 
            if <i>&lt;WellRow&gt;</i> is "A" and <i>&lt;WellColumn&gt;</i> is "01", a field 
            <i>&lt;Well&gt;</i> will be "A01". This is useful if your well row and column names are
            separated from each other in the filename, but you want to retain the standard 
            well nomenclature.</p>"""))
   
        group.append("folder_regexp", cps.RegexpText(
            "Regular expression",
            '(?P<Date>[0-9]{4}_[0-9]{2}_[0-9]{2})$',
            get_example_fn = self.example_directory_fn,
            doc="""
            <i>(Used only if you want to extract metadata from the path)</i><br>
            Enter the regular expression for extracting the metadata from the 
            path. Note that this field is available whether you have selected 
            <i>Text-Regular expressions</i> to load the files or not.
            
            <p>Clicking the magnifying glass icon to the right will bring up a
            tool that will allow you to check the accuracy of your regular 
            expression. The regular expression syntax can be used to 
            name different parts of your expression. The syntax 
            <i>(?&lt;fieldname&gt;expr)</i> will extract whatever matches 
            <i>expr</i> and assign it to the image's <i>fieldname</i> measurement.
                        
            <p>For instance, a researcher uses folder names with the date and 
            subfolders containing the images with the run ID 
            (e.g., <i>./2009_10_02/1234/</i>) The following regular expression 
            will capture the plate, well, and site in the fields 
            <i>Date</i> and <i>Run</i>:<br>
            <table border = "1">
            <tr><td colspan = "2">.*[\\\/](?P&lt;Date&gt;.*)[\\\\/](?P&lt;Run&gt;.*)$</td></tr>
            <tr><td>.*[\\\\/]</td><td>Skip characters at the beginning of the pathname until either a slash (/) or
            backslash (\\) is encountered (depending on the operating system)</td></tr>
            <tr><td>(?P&lt;Date&gt;</td><td>Name the captured field <i>Date</i></td></tr>
            <tr><td>.*</td><td>Capture as many characters that follow</td></tr>
            <tr><td>[\\\\/]</td><td>Discard the slash/backslash character</td></tr>
            <tr><td>(?P&lt;Run&gt;</td><td>Name the captured field <i>Run</i></td></tr>
            <tr><td>.*</td><td>Capture as many characters as follow</td></tr>
            <tr><td>$</td><td>The <i>Run</i> field must be at the end of the path string, i.e., the
            last folder on the path. This also means that the Date field contains the parent
            folder of the Date folder.</td></tr>
            </table></p>"""))
 
        group.append("filter_choice", cps.Choice(
            "Filter images",
            [F_ALL_IMAGES, F_FILTERED_IMAGES],
            doc = """Do you want to extract data from all of the images
            chosen by the <b>Images</b> module or on a subset of the images?"""))
        
        group.append("filter", cps.Filter(
            "", [FilePredicate(),
                 DirectoryPredicate(),
                 ExtensionPredicate()],
            'or (file does contain "")',
            doc = """Pick the files for metadata extraction."""))
        
        group.append("csv_location", cps.Pathname(
            "Metadata file location:",
            wildcard="Metadata files (*.csv)|*.csv|All files (*.*)|*.*"))
        
        group.append("csv_joiner", cps.Joiner(
            "Match file and image metadata", allow_none = False,
            doc="""Match columns in your .csv file to image metadata items
            <hr>
            This setting controls how rows in your .csv file are matched to
            different images. The setting displays the columns in your
            .csv file in one of its columns and the metadata in your images
            in the other, including the metadata extracted by previous
            metadata extractors in this module.
            """))
        
        group.append("update_metadata", cps.DoSomething(
            "Update", "Update metadata",
            lambda : self.do_update_metadata(group),
            doc = """Press this button to automatically extract metadata from
            your image files."""))
                 
        group.can_remove = can_remove
        if can_remove:
            group.append("remover", cps.RemoveSettingButton(
                'Remove above extraction method', 'Remove',
                self.extraction_methods, group))
            
    def settings(self):
        result = [self.wants_metadata, self.extraction_method_count]
        for group in self.extraction_methods:
            result += [
                group.extraction_method, group.source, group.file_regexp,
                group.folder_regexp, group.filter_choice, group.filter,
                group.csv_location, group.csv_joiner]
        return result
    
    def visible_settings(self):
        result = [self.metadata_explanation, self.wants_metadata]
        if self.wants_metadata:
            for group in self.extraction_methods:
                if group.can_remove:
                    result += [group.divider]
                result += [group.extraction_method]
                if group.extraction_method == X_MANUAL_EXTRACTION:
                    result += [group.source]
                    if group.source == XM_FILE_NAME:
                        result += [group.file_regexp]
                    elif group.source == XM_FOLDER_NAME:
                        result += [group.folder_regexp]
                    result += [group.filter_choice]
                    if group.filter_choice == F_FILTERED_IMAGES:
                        result += [group.filter]
                elif group.extraction_method == X_IMPORTED_EXTRACTION:
                    result += [group.csv_location, group.filter_choice]
                    if group.filter_choice == F_FILTERED_IMAGES:
                        result += [group.filter]
                    result += [group.csv_joiner]
                elif group.extraction_method == X_AUTOMATIC_EXTRACTION:
                    result += [group.filter_choice]
                    if group.filter_choice == F_FILTERED_IMAGES:
                        result += [group.filter]
                    result += [group.update_metadata]
                if group.can_remove:
                    result += [group.remover]
            result += [self.add_extraction_method_button,
                       self.update_table_button, self.table]
        return result
    
    def example_file_fn(self):
        '''Get an example file name for the regexp editor'''
        if len(self.ipds) > 0:
            return os.path.split(self.ipds[0].path)[1]
        return "PLATE_A01_s1_w11C78E18A-356E-48EC-B204-3F4379DC43AB.tif"
            
    def example_directory_fn(self):
        '''Get an example directory name for the regexp editor'''
        if len(self.ipds) > 0:
            return os.path.split(self.ipds[0].path)[0]
        return "/images/2012_01_12"
    
    def change_causes_prepare_run(self, setting):
        '''Return True if changing the setting passed changes the image sets
        
        setting - the setting that was changed
        '''
        return setting in self.settings()
    
    def prepare_run(self, workspace):
        '''Initialize the pipeline's metadata'''
        from subimager.omexml import OMEXML
        
        file_list = workspace.file_list
        pipeline = workspace.pipeline
        self.ipd_metadata_keys = []
        self.update_imported_metadata()
        for ipd in pipeline.image_plane_details:
            metadata = self.get_ipd_metadata(ipd)
            ipd.metadata.update(metadata)
            metadata = file_list.get_metadata(ipd.url)
            if metadata is not None:
                try:
                    pipeline.add_image_metadata(ipd.url, OMEXML(metadata))
                except:
                    logger.error("Failed to add metadata to %s" %ipd.url, 
                                 exc_info=True)
        return True
        
    def run(self, workspace):
        pass
    
    def do_update_metadata(self, group):
        filelist = self.workspace.file_list
        urls = self.pipeline.filter_urls(filelist.get_filelist())
        if len(urls) == 0:
            return
        def msg(url):
            return "Processing %s" % url
        import wx
        from subimager.client import get_metadata
        from subimager.omexml import OMEXML
        with wx.ProgressDialog("Updating metadata", 
                               msg(urls[0]),
                               len(urls),
                               style = wx.PD_CAN_ABORT
                               | wx.PD_APP_MODAL
                               | wx.PD_ELAPSED_TIME
                               | wx.PD_REMAINING_TIME) as dlg:
            for i, url in enumerate(urls):
                if i > 0:
                    keep_going, _ = dlg.Update(i, msg(url))
                    if not keep_going:
                        break
                if group.filter_choice == F_FILTERED_IMAGES:
                    match = group.filter.evaluate(
                    (cps.FileCollectionDisplay.NODE_IMAGE_PLANE, 
                     Images.url_to_modpath(url), self))
                    if not match:
                        continue
                metadata = filelist.get_metadata(url)
                if metadata is None:
                    metadata = get_metadata(url)
                    filelist.add_metadata(url, metadata)
                metadata = OMEXML(metadata)
                exemplar = cpp.ImagePlaneDetails(url, None, None, None)
                if not self.pipeline.find_image_plane_details(exemplar):
                    self.pipeline.add_image_plane_details([exemplar])
                self.pipeline.add_image_metadata(url, metadata)
            self.ipds = self.pipeline.get_filtered_image_plane_details()
            self.update_metadata_keys()
                
    def get_ipd_metadata(self, ipd):
        '''Get the metadata for an image plane details record'''
        assert isinstance(ipd, cpp.ImagePlaneDetails)
        m = ipd.metadata.copy()
        for group in self.extraction_methods:
            if group.filter_choice == F_FILTERED_IMAGES:
                match = group.filter.evaluate(
                    (cps.FileCollectionDisplay.NODE_IMAGE_PLANE, 
                     Images.url_to_modpath(ipd.url), self))
                if (not match) and match is not None:
                    continue
            if group.extraction_method == X_MANUAL_EXTRACTION:
                m.update(self.manually_extract_metadata(group, ipd))
            elif group.extraction_method == X_AUTOMATIC_EXTRACTION:
                m.update(self.automatically_extract_metadata(group, ipd))
            elif group.extraction_method == X_IMPORTED_EXTRACTION:
                m.update(self.import_metadata(group, ipd, m))
        return m
                
    def manually_extract_metadata(self, group, ipd):
        if group.source == XM_FILE_NAME:
            text = os.path.split(ipd.path)[1]
            pattern = group.file_regexp.value
        elif group.source == XM_FOLDER_NAME:
            text = os.path.split(ipd.path)[0]
            pattern = group.folder_regexp.value
        else:
            return {}
        match = re.search(pattern, text)
        if match is None:
            return {}
        return match.groupdict()
    
    def automatically_extract_metadata(self, group, ipd):
        return {}
    
    def import_metadata(self, group, ipd, m):
        for imported_metadata in self.imported_metadata:
            assert isinstance(imported_metadata, self.ImportedMetadata)
            if imported_metadata.is_match(
                group.csv_location.value,
                group.csv_joiner,
                self.CSV_JOIN_NAME,
                self.IPD_JOIN_NAME):
                return imported_metadata.get_ipd_metadata(m)
                
        return {}
    
    def on_activated(self, workspace):
        self.workspace = workspace
        self.pipeline = workspace.pipeline
        self.pipeline.load_image_plane_details(workspace)
        self.ipds = self.pipeline.get_filtered_image_plane_details()
        self.ipd_metadata_keys = []
        self.update_metadata_keys()
        self.update_imported_metadata()
        self.update_table()
        
    def on_setting_changed(self, setting, pipeline):
        self.update_imported_metadata()
        
    def update_imported_metadata(self):
        new_imported_metadata = []
        ipd_metadata_keys = set(self.ipd_metadata_keys)
        for group in self.extraction_methods:
            if group.extraction_method == X_MANUAL_EXTRACTION:
                if group.source == XM_FILE_NAME:
                    regexp = group.file_regexp
                else:
                    regexp = group.folder_regexp
                ipd_metadata_keys.update(cpmeas.find_metadata_tokens(regexp.value))
            elif group.extraction_method == X_IMPORTED_EXTRACTION:
                joiner = group.csv_joiner
                csv_path = group.csv_location.value
                if not os.path.isfile(csv_path):
                    continue
                found = False
                best_match = None
                for i, imported_metadata in enumerate(self.imported_metadata):
                    assert isinstance(imported_metadata, self.ImportedMetadata)
                    if imported_metadata.is_match(csv_path, joiner,
                                                  self.CSV_JOIN_NAME,
                                                  self.IPD_JOIN_NAME):
                        new_imported_metadata.append(imported_metadata)
                        found = True
                        break
                    elif (best_match is None and 
                          imported_metadata.path == csv_path):
                        best_match = i
                if found:
                    del self.imported_metadata[i]
                else:
                    if best_match is not None:
                        imported_metadata = self.imported_metadata[i]
                        del self.imported_metadata[i]
                    else:
                        try:
                            imported_metadata = self.ImportedMetadata(csv_path)
                        except:
                            logger.debug("Failed to load csv file: %s" % csv_path)
                            continue
                    joiner.entities[self.CSV_JOIN_NAME] = \
                        imported_metadata.get_csv_metadata_keys()
                    joiner.entities[self.IPD_JOIN_NAME] = \
                        list(ipd_metadata_keys)
                    imported_metadata.set_joiner(joiner,
                                                 self.CSV_JOIN_NAME,
                                                 self.IPD_JOIN_NAME)
                    new_imported_metadata.append(imported_metadata)
                ipd_metadata_keys.update(imported_metadata.get_csv_metadata_keys())
                    
        self.imported_metadata = new_imported_metadata            
        
    def update_table(self):
        columns = set()
        for ipd in self.ipds:
            for column in self.get_ipd_metadata(ipd).keys():
                columns.add(column)
        columns = [COL_PATH, COL_SERIES, COL_INDEX, COL_CHANNEL] + \
            sorted(list(columns))
        self.table.clear_columns()
        self.table.clear_rows()
        for i, column in enumerate(columns):
            self.table.insert_column(i, column)
            
        data = []
        for ipd in self.ipds:
            row = [ipd.path, ipd.series, ipd.index, ipd.channel]
            metadata = self.get_ipd_metadata(ipd)
            row += [metadata.get(column) for column in columns[4:]]
            data.append(row)
        self.table.add_rows(columns, data)
        
    def update_metadata_keys(self):
        self.ipd_metadata_keys = set(self.ipd_metadata_keys)
        for ipd in self.ipds:
            self.ipd_metadata_keys.update(ipd.metadata.keys())
        self.ipd_metadata_keys = sorted(self.ipd_metadata_keys)
        
    def on_deactivated(self):
        self.pipeline = None
        
    def prepare_settings(self, setting_values):
        '''Prepare the module to receive the settings'''
        #
        # Set the number of extraction methods based on the extraction method
        # count.
        #
        n_extraction_methods = int(setting_values[IDX_EXTRACTION_METHOD_COUNT])
        if len(self.extraction_methods) > n_extraction_methods:
            del self.extraction_methods[n_extraction_methods:]
            
        while len(self.extraction_methods) < n_extraction_methods:
            self.add_extraction_method()
            
    def get_metadata_keys(self):
        '''Return a collection of metadata keys to be associated with files'''
        keys = set()
        for group in self.extraction_methods:
            if group.extraction_method == X_MANUAL_EXTRACTION:
                if group.source == XM_FILE_NAME:
                    regexp = group.file_regexp
                else:
                    regexp = group.folder_regexp
                keys.update(cpmeas.find_metadata_tokens(regexp.value))
            elif group.extraction_method == X_IMPORTED_EXTRACTION:
                # TO-DO: come up with yet another bogus cacheing strategy
                try:
                    with open(group.csv_location.value, "r") as fd:
                        rdr = csv.reader(fd)
                        header = rdr.next()
                        keys.update(header)
                except:
                    logger.debug("Failed to read %s" % group.csv_location.value)
        return list(keys)
    
    def get_measurement_columns(self, pipeline):
        '''Get the metadata measurements collected by this module'''
        return [ (cpmeas.IMAGE, 
                  '_'.join((cpmeas.C_METADATA, key)),
                  cpmeas.COLTYPE_VARCHAR_FILE_NAME)
                 for key in self.get_metadata_keys()]
    
    class ImportedMetadata(object):
        '''A holder for the metadata from a csv file'''
        def __init__(self, path):
            self.joiner_initialized = False
            self.path = path
            self.path_timestamp = os.stat(path).st_mtime
            fd = open(path, "r")
            rdr = csv.reader(fd)
            header = rdr.next()
            columns = [[] for  c in header]
            self.columns = dict([(c, l) for c,l in zip(header, columns)])
            for row in rdr:
                for i, field in enumerate(row):
                    columns[i].append(None if len(field) == 0 else field)
                if len(row) < len(columns):
                    for i in range(len(row), len(columns)):
                        columns[i].append(None)
                        
        def get_csv_metadata_keys(self):
            '''Get the metadata keys in the CSV header'''
            return sorted(self.columns.keys())
        
        def set_joiner(self, joiner, csv_name, ipd_name):
            '''Initialize to assign csv metadata to an image plane descriptor
            
            joiner - a joiner setting that describes the join between the
                     CSV metadata and the IPD metadata
            csv_name - the name assigned to the CSV file in the joiner
            ipd_name - the name assigned to the IPD in the joiner
            
            Creates a dictionary of keys from the CSV joining keys and
            records the keys that will be used to join in the IPD
            '''
            joins = joiner.parse()
            if len(joins) == 0:
                return
            if any([join.get(csv_name) not in self.columns.keys()
                    for join in joins]):
                return
            self.csv_keys = [join[csv_name] for join in joins]
            self.ipd_keys = [join[ipd_name] for join in joins]
            self.metadata_keys = set(self.columns.keys()).difference(self.csv_keys)
            self.d = {}
            columns = [self.columns[key] for key in self.csv_keys]
            for i in range(len(columns[0])):
                key = tuple([column[i] for column in columns])
                self.d[key] = i
            self.joiner_initialized = True
            
        def get_ipd_metadata(self, ipd_metadata):
            '''Get the matching metadata from the .csv for a given ipd
            
            ipd_metadata - the metadata dictionary for an IPD, possibly
            augmented by prior extraction
            '''
            if not self.joiner_initialized:
                return {}
            key = tuple([ipd_metadata.get(k) for k in self.ipd_keys])
            if not self.d.has_key(key):
                return {}
            return dict([(k, self.columns[k][self.d[key]]) 
                         for k in self.metadata_keys])
        
        def is_match(self, csv_path, joiner, csv_join_name, ipd_join_name):
            '''Check to see if this instance can handle the given csv and joiner
            
            csv_path - path to the CSV file to use
            
            joiner - the joiner to join to the ipd metadata
            
            csv_join_name - the join name in the joiner of the csv_file
            
            ipd_join_name - the join name in the joiner of the ipd metadata
            '''
            if csv_path != self.path:
                return False
            
            if os.stat(self.path).st_mtime != self.path_timestamp:
                return False
            
            if not self.joiner_initialized:
                self.set_joiner(joiner, csv_join_name, ipd_join_name)
                return True
            
            joins = joiner.parse()
            csv_keys = [join[csv_join_name] for join in joins]
            ipd_keys = [join[ipd_join_name] for join in joins]
            metadata_keys = set(self.columns.keys()).difference(self.csv_keys)
            for mine, yours in ((self.csv_keys, csv_keys),
                                (self.ipd_keys, ipd_keys),
                                (self.metadata_keys, metadata_keys)):
                if tuple(mine) != tuple(yours):
                    return False
            return True
            
            
