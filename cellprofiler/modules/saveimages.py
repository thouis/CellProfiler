'''<b>Save Images </b> saves image or movie files
<hr>

Because CellProfiler usually performs many image analysis steps on many
groups of images, it does <i>not</i> save any of the resulting images to the
hard drive unless you specifically choose to do so with the <b>SaveImages</b> 
module. You can save any of the
processed images created by CellProfiler during the analysis using this module.

<p>You can choose from many different image formats for saving your files. This
allows you to use the module as a file format converter, by loading files
in their original format and then saving them in an alternate format.

<p>Note that saving images in 12-bit format is not supported, and 16-bit format
is supported for TIFF only.
<p>
See also <b>LoadImages</b>, <b>ConserveMemory</b>.
'''

# CellProfiler is distributed under the GNU General Public License.
# See the accompanying file LICENSE for details.
# 
# Copyright (c) 2003-2009 Massachusetts Institute of Technology
# Copyright (c) 2009-2012 Broad Institute
# 
# Please see the AUTHORS file for credits.
# 
# Website: http://www.cellprofiler.org

__version__="$Revision$"

import logging
import matplotlib
import numpy as np
import re
import os
import sys
import scipy.io.matlab.mio
import traceback

logger = logging.getLogger(__name__)

import cellprofiler.cpmodule as cpm
import cellprofiler.measurements as cpmeas
import cellprofiler.settings as cps
import cellprofiler.preferences as cpp
from cellprofiler.gui.help import USING_METADATA_TAGS_REF, USING_METADATA_HELP_REF
from cellprofiler.preferences import \
     standardize_default_folder_names, DEFAULT_INPUT_FOLDER_NAME, \
     DEFAULT_OUTPUT_FOLDER_NAME, ABSOLUTE_FOLDER_NAME, \
     DEFAULT_INPUT_SUBFOLDER_NAME, DEFAULT_OUTPUT_SUBFOLDER_NAME, \
     IO_FOLDER_CHOICE_HELP_TEXT, IO_WITH_METADATA_HELP_TEXT
from cellprofiler.utilities.relpath import relpath
from cellprofiler.modules.loadimages import C_FILE_NAME, C_PATH_NAME
from cellprofiler.modules.loadimages import C_OBJECTS_FILE_NAME, C_OBJECTS_PATH_NAME
from cellprofiler.modules.loadimages import pathname2url
from cellprofiler.cpmath.cpmorphology import distance_color_labels
from subimager.client import post_image
import subimager.omexml as ome
from cellprofiler.utilities.version import get_version

IF_IMAGE       = "Image"
IF_MASK        = "Mask"
IF_CROPPING    = "Cropping"
IF_FIGURE      = "Module window"
IF_MOVIE       = "Movie"
IF_OBJECTS     = "Objects"
IF_ALL = [IF_IMAGE, IF_MASK, IF_CROPPING, IF_MOVIE, IF_FIGURE, IF_OBJECTS]

BIT_DEPTH_8 = "8"
BIT_DEPTH_16 = "16"

FN_FROM_IMAGE  = "From image filename"
FN_SEQUENTIAL  = "Sequential numbers"
FN_SINGLE_NAME = "Single name"
SINGLE_NAME_TEXT = "Enter single file name"
FN_WITH_METADATA = "Name with metadata"
FN_IMAGE_FILENAME_WITH_METADATA = "Image filename with metadata"
METADATA_NAME_TEXT = ("""Enter file name with metadata""")
SEQUENTIAL_NUMBER_TEXT = "Enter file prefix"
FF_BMP         = "bmp"
FF_HDF         = "hdf"
FF_JPG         = "jpg"
FF_JPEG        = "jpeg"
FF_PBM         = "pbm"
FF_PCX         = "pcx"
FF_PGM         = "pgm"
FF_PNG         = "png"
FF_PNM         = "pnm"
FF_PPM         = "ppm"
FF_RAS         = "ras"
FF_TIF         = "tif"
FF_TIFF        = "tiff"
FF_XWD         = "xwd"
FF_AVI         = "avi"
FF_MAT         = "mat"
FF_MOV         = "mov"
FF_SUPPORTING_16_BIT = [FF_TIF, FF_TIFF]
PC_WITH_IMAGE  = "Same folder as image"
OLD_PC_WITH_IMAGE_VALUES = ["Same folder as image"]
PC_CUSTOM      = "Custom"
PC_WITH_METADATA = "Custom with metadata"
WS_EVERY_CYCLE = "Every cycle"
WS_FIRST_CYCLE = "First cycle"
WS_LAST_CYCLE  = "Last cycle"
CM_GRAY        = "gray"

GC_GRAYSCALE = "Grayscale"
GC_COLOR = "Color"

'''Offset to the directory path setting'''
OFFSET_DIRECTORY_PATH = 10
class SaveImages(cpm.CPModule):

    module_name = "SaveImages"
    variable_revision_number = 7
    category = "File Processing"
    
    def create_settings(self):
        self.save_image_or_figure = cps.Choice("Select the type of image to save",
                                               IF_ALL,
                                               IF_IMAGE,doc="""
                The following types of images can be saved as a file on the hard drive:
                <ul>
                <li><i>Image:</i> Any of the images produced upstream of <b>SaveImages</b> can be selected for saving. 
                Outlines created by <b>Identify</b> modules can also be saved with this option, but you must 
                select "Retain outlines..." of identified objects within the <b>Identify</b> module. You might
                also want to use the <b>OverlayOutlines</b> module prior to saving images.</li>
                <li><i>Crop mask (Relevant only if the Crop module is used):</i> The <b>Crop</b> module 
                creates a mask of the pixels of interest in the image. Saving the mask will produce a 
                binary image in which the pixels of interest are set to 1; all other pixels are 
                set to 0.</li>
                <li><i>Image's cropping (Relevant only if the Crop module is used):</i> The <b>Crop</b> 
                module also creates a cropping image which is typically the same size as the original 
                image. However, since the <b>Crop</b> permits removal of the rows and columns that are left 
                blank, the cropping can be of a different size than the mask.</li>
                <li><i>Movie:</i> A sequence of images can be saved as a movie file. Currently only AVIs can be written. 
                Each image becomes a frame of the movie.</li>
                <li><i>Objects:</i> Objects can be saved as an image. The image
                is saved as grayscale unless you select a color map other than 
                gray. Background pixels appear as black and
                each object is assigned an intensity level corresponding to
                its object number. The resulting image can be loaded as objects
                by the <b>LoadImages</b> module. Objects are best saved as .tif
                files. <b>SaveImages</b> will use an 8-bit .tif file if there
                are fewer than 256 objects and will use a 16-bit .tif otherwise.
                Results may be unpredictable if you save using .png and there
                are more than 255 objects or if you save using one of the other
                file formats.</li>
                <li><i>Module display window:</i> The window associated with a module can be saved, which
                will include all the panels and text within that window. <b>Currently, this option is not yet available.</b></li>
                </ul>""")
        
        self.image_name  = cps.ImageNameSubscriber("Select the image to save","None", doc = """
                <i>(Used only if saving images, crop masks, and image croppings)</i><br>
                What did you call the images you want to save?""")
        
        self.objects_name = cps.ObjectNameSubscriber(
            "Select the objects to save", "None",
            doc = """<i>(Used only if saving objects)</i><br>
            This setting chooses which objects should be saved.""")
        
        self.figure_name = cps.FigureSubscriber("Select the module display window to save","None",doc="""
                <i>(Used only if saving module display windows)</i><br>
                Enter the module number/name for which you want to save the module display window.""")
        
        self.file_name_method = cps.Choice("Select method for constructing file names",
                                           [FN_FROM_IMAGE, FN_SEQUENTIAL,
                                            FN_SINGLE_NAME],
                                            FN_FROM_IMAGE,doc="""
                <i>(Used only if saving non-movie files)</i><br>
                Three choices are available:
                <ul>
                <li><i>From image filename:</i> The filename will be constructed based
                on the original filename of an input image specified in <b>LoadImages</b>
                or <b>LoadData</b>. You will have the opportunity to prefix or append
                additional text. <br>
                If you have metadata associated with your images, you can append an text 
                to the image filename using a metadata tag. This is especially useful if you 
                want your output given a unique label according to the metadata corresponding 
                to an image group. The name of the metadata to substitute can be extracted from
                the image filename each cycle using <b>LoadImages</b> or provided for each image using 
                <b>LoadData</b>. %(USING_METADATA_TAGS_REF)s%(USING_METADATA_HELP_REF)s.</li>
                <li><i>Sequential numbers:</i> Same as above, but in addition, each filename
                will have a number appended to the end that corresponds to
                the image cycle number (starting at 1).</li>
                <li><i>Single name:</i> A single name will be given to the
                file. Since the filename is fixed, this file will be overwritten with each cycle. 
                In this case, you would probably want to save the image on the last cycle 
                (see the <i>Select how often to save</i> setting)<br>
                The exception to this is to use a metadata tag to provide a unique label, as mentioned 
                in the <i>From image filename</i> option.</li>
                </ul>"""%globals())
        
        self.file_image_name = cps.FileImageNameSubscriber("Select image name for file prefix",
                                                           "None",doc="""
                <i>(Used only when constructing the filename from the image filename, with or without metadata)</i><br>
                Select an image loaded using <b>LoadImages</b> or <b>LoadData</b>. The original filename will be
                used as the prefix for the output filename.""")
        
        self.single_file_name = cps.Text(SINGLE_NAME_TEXT, "OrigBlue",
                                         metadata = True,
                                         doc="""
                <i>(Used only when constructing the filename from the image filename, a single name or a name with metadata)</i><br>
                If you are constructing the filenames using...
                <ul>
                <li><i>Single name:</i> Enter the filename text here</li>
                <li><i>Custom with metadata:</i> If you have metadata 
                associated with your images, enter the filename text with the metadata tags. %(USING_METADATA_TAGS_REF)s.   
                For example, if the <i>plate</i>, <i>well_row</i> and <i>well_column</i> tags have the values <i>XG45</i>, <i>A</i>
                and <i>01</i>, respectively, the string "Illum_\g&lt;plate&gt;_\g&lt;well_row&gt;\g&lt;well_column&gt;"
                produces the output filename <i>Illum_XG45_A01</i>.</li>
                </ul>
                Do not enter the file extension in this setting; it will be appended automatically."""%globals())
        
        self.wants_file_name_suffix = cps.Binary(
            "Do you want to add a suffix to the image file name?", False,
            doc = """Check this setting to add a suffix to the image's file name.
            Leave the setting unchecked to use the image name as-is.""")
        
        self.file_name_suffix = cps.Text("Text to append to the image name",
                                         "", metadata = True,
                                         doc="""
                <i>(Used only when constructing the filename from the image filename)</i><br>
                Enter the text that should be appended to the filename specified above.""")
        
        self.file_format = cps.Choice("Select file format to use",
                                      [FF_TIF,FF_BMP,FF_HDF,FF_JPG,FF_JPEG,
                                       FF_PBM,FF_PCX,FF_PGM,FF_PNG,FF_PNM,
                                       FF_PPM,FF_RAS,FF_TIF,FF_TIFF,FF_XWD,
                                       FF_MAT],FF_BMP,doc="""
                <i>(Used only when saving non-movie files)</i><br>
                Select the image or movie format to save the image(s). Most common
                image formats are available; MAT-files are readable by MATLAB.""")
        
        self.pathname = SaveImagesDirectoryPath(
            "Output file location", self.file_image_name,
            doc = """ 
                <i>(Used only when saving non-movie files)</i><br>
                This setting lets you choose the folder for the output
                files. %(IO_FOLDER_CHOICE_HELP_TEXT)s
                <p>An additional option is the following:
                <ul>
                <li><i>Same folder as image</i>: Place the output file in the same folder
                that the source image is located.</li>
                </ul></p>
                <p>%(IO_WITH_METADATA_HELP_TEXT)s %(USING_METADATA_TAGS_REF)s. 
                For instance, if you have a metadata tag named 
                "Plate", you can create a per-plate folder by selecting one the subfolder options
                and then specifying the subfolder name as "\g&lt;Plate&gt;". The module will 
                substitute the metadata values for the current image set for any metadata tags in the 
                folder name.%(USING_METADATA_HELP_REF)s.</p>
                <p>If the subfolder does not exist when the pipeline is run, CellProfiler will
                create it.</p>
                <p>If you are creating nested subfolders using the sub-folder options, you can 
                specify the additional folders separated with slashes. For example, "Outlines/Plate1" will create
                a "Plate1" folder in the "Outlines" folder, which in turn is under the Default
                Input/Output Folder. The use of a forward slash ("/") as a folder separator will 
                avoid ambiguity between the various operating systems.</p>"""%globals())
        
        # TODO: 
        self.bit_depth = cps.Choice("Image bit depth",
                [BIT_DEPTH_8, BIT_DEPTH_16],doc="""
                <i>(Used only when saving files in a non-MAT format)</i><br>
                What is the bit-depth at which you want to save the images?
                <b>16-bit images are supported only for TIF formats.
                Currently, saving images in 12-bit is not supported.</b>""")
        
        self.overwrite = cps.Binary("Overwrite existing files without warning?",False,doc="""
                Check this box to automatically overwrite a file if it already exists. Otherwise, you
                will be prompted for confirmation first. If you are running the pipeline on a computing cluster,
                you should uncheck this box since you will not be able to intervene and answer the confirmation prompt.""")
        
        self.when_to_save = cps.Choice("Select how often to save",
                [WS_EVERY_CYCLE,WS_FIRST_CYCLE,WS_LAST_CYCLE],
                WS_EVERY_CYCLE,doc="""<a name='when_to_save'>
                <i>(Used only when saving non-movie files)</i><br>
                Specify at what point during pipeline execution to save file(s). </a>
                <ul>
                <li><i>Every cycle:</i> Useful for when the image of interest is created every cycle and is
                not dependent on results from a prior cycle.</li>
                <li><i>First cycle:</i> Useful for when you are saving an aggregate image created 
                on the first cycle, e.g., <b>CorrectIlluminationCalculate</b> with the <i>All</i>
                setting used on images obtained directly from <b>LoadImages</b>/<b>LoadData</b></a>.</li>
                <li><i>Last cycle:</i> Useful for when you are saving an aggregate image completed 
                on the last cycle, e.g., <b>CorrectIlluminationCalculate</b> with the <i>All</i>
                setting used on intermediate images generated during each cycle.</li>
                </ul> """)
        
        self.rescale = cps.Binary("Rescale the images? ",False,doc="""
                <i>(Used only when saving non-MAT file images)</i><br>
                Check this box if you want the image to occupy the full dynamic range of the bit 
                depth you have chosen. For example, if you save an image to an 8-bit file, the
                smallest grayscale value will be mapped to 0 and the largest value will be mapped 
                to 2<sup>8</sup>-1 = 255. 
                <p>This will increase the contrast of the output image but will also effectively 
                stretch the image data, which may not be desirable in some 
                circumstances. See <b>RescaleIntensity</b> for other rescaling options.</p>""")
        
        self.gray_or_color = cps.Choice(
            "Save as grayscale or color image?",
            [GC_GRAYSCALE, GC_COLOR],
            doc = """<i>(Used only when saving objects)</i><br>
            You can save objects as a grayscale image or as a color image.
            <b>SaveImages</b> uses a pixel's object number as the grayscale
            intensity in a grayscale image with background pixels being
            colored black. It assigns different colors to different objects
            if you choose to save as a color image. Grayscale images are more
            suitable if you are going to load the image as objects using
            <b>LoadImages</b> or some other program that will be used to
            relate object measurements to the pixels in the image.<br>
            You should save grayscale images using the .TIF or .MAT formats
            if possible; otherwise you may have problems saving files
            with more than 255 objects.""")
        
        self.colormap = cps.Colormap('Select colormap', 
                                     value = CM_GRAY,
                                     doc= """
                <i>(Used only when saving non-MAT file images)</i><br>
                This affects how images color intensities are displayed. All available colormaps can be seen 
                <a href="http://www.scipy.org/Cookbook/Matplotlib/Show_colormaps">here</a>.""")
        
        self.update_file_names = cps.Binary("Store file and path information to the saved image?",False,doc="""
                This setting stores filename and pathname data for each of the new files created 
                via this module, as a per-image measurement. Instances in which this information may 
                be useful include:
                <ul>
                <li>Exporting measurements to a database, allowing 
                access to the saved image. If you are using the machine-learning tools or image
                viewer in CellProfiler Analyst, for example, you will want to check this box if you want
                the images you are saving via this module to be displayed along with the original images.</li>
                <li>Allowing downstream modules (e.g., <b>CreateWebPage</b>) to access  
                the newly saved files.</li>
                </ul>""")
        
        self.create_subdirectories = cps.Binary("Create subfolders in the output folder?",False,
                                                doc = """Subfolders will be created to match the input image folder structure.""")
    
    def settings(self):
        """Return the settings in the order to use when saving"""
        return [self.save_image_or_figure, self.image_name, 
                self.objects_name, self.figure_name,
                self.file_name_method, self.file_image_name,
                self.single_file_name, self.wants_file_name_suffix, 
                self.file_name_suffix, self.file_format,
                self.pathname, self.bit_depth,
                self.overwrite, self.when_to_save,
                self.rescale, self.gray_or_color, self.colormap, 
                self.update_file_names, self.create_subdirectories]
    
    def visible_settings(self):
        """Return only the settings that should be shown"""
        result = [self.save_image_or_figure]
        if self.save_image_or_figure == IF_FIGURE:
            result.append(self.figure_name)
        elif self.save_image_or_figure == IF_OBJECTS:
            result.append(self.objects_name)
        else:
            result.append(self.image_name)

        result.append(self.file_name_method)
        if self.file_name_method == FN_FROM_IMAGE:
            result += [self.file_image_name, self.wants_file_name_suffix]
            if self.wants_file_name_suffix:
                result.append(self.file_name_suffix)
        elif self.file_name_method == FN_SEQUENTIAL:
            self.single_file_name.text = SEQUENTIAL_NUMBER_TEXT
            # XXX - Change doc, as well!
            result.append(self.single_file_name)
        elif self.file_name_method == FN_SINGLE_NAME:
            self.single_file_name.text = SINGLE_NAME_TEXT
            result.append(self.single_file_name)
        else:
            raise NotImplementedError("Unhandled file name method: %s"%(self.file_name_method))
        if self.save_image_or_figure != IF_MOVIE:
            result.append(self.file_format)
        if (self.file_format in FF_SUPPORTING_16_BIT and 
            self.save_image_or_figure == IF_IMAGE):
            # TIFF supports 8 & 16-bit, all others are written 8-bit
            result.append(self.bit_depth)
        result.append(self.pathname)
        result.append(self.overwrite)
        if self.save_image_or_figure != IF_MOVIE:
            result.append(self.when_to_save)
        if (self.save_image_or_figure == IF_IMAGE and
            self.file_format != FF_MAT):
            result.append(self.rescale)
            result.append(self.colormap)
        elif self.save_image_or_figure == IF_OBJECTS:
            result.append(self.gray_or_color)
            if self.gray_or_color == GC_COLOR:
                result.append(self.colormap)
        result.append(self.update_file_names)
        result.append(self.create_subdirectories)
        return result
    
    @property
    def module_key(self):
        return "%s_%d"%(self.module_name, self.module_num)
    
    def prepare_run(self, workspace, *args):
        workspace.image_set_list.legacy_fields[self.module_key] = {}
        return True

    def prepare_group(self, workspace, grouping, image_numbers):
        d = self.get_dictionary(workspace.image_set_list)
        d['FIRST_IMAGE'] = True
        if self.save_image_or_figure == IF_MOVIE:
            d['N_FRAMES'] = len(image_numbers)
            d['CURRENT_FRAME'] = 0
        return True
    
    def prepare_to_create_batch(self, workspace, fn_alter_path):
        self.pathname.alter_for_create_batch_files(fn_alter_path)
        
    def run(self,workspace):
        """Run the module
        
        pipeline     - instance of CellProfiler.Pipeline for this run
        workspace    - the workspace contains:
            image_set    - the images in the image set being processed
            object_set   - the objects (labeled masks) in this image set
            measurements - the measurements for this run
            frame        - display within this frame (or None to not display)
        """
        if self.save_image_or_figure.value in (IF_IMAGE, IF_MASK, IF_CROPPING):
            should_save = self.run_image(workspace)
        elif self.save_image_or_figure == IF_MOVIE:
            should_save = self.run_movie(workspace)
        elif self.save_image_or_figure == IF_OBJECTS:
            should_save = self.run_objects(workspace)
        else:
            raise NotImplementedError(("Saving a %s is not yet supported"%
                                       (self.save_image_or_figure)))
        workspace.display_data.filename = self.get_filename(
            workspace, make_dirs = False, check_overwrite = False)
        
    def display(self, workspace, figure):
        if self.show_window:
            if self.save_image_or_figure == IF_MOVIE:
                return
            figure.set_subplots((1, 1))
            outcome = ("Wrote %s" if workspace.display_data.wrote_image
                       else "Did not write %s")
            figure.subplot_table(0, 0, [[outcome %
                                         (workspace.display_data.filename)]])

    
    def run_image(self,workspace):
        """Handle saving an image"""
        #
        # First, check to see if we should save this image
        #
        if self.when_to_save == WS_FIRST_CYCLE:
            d = self.get_dictionary(workspace.image_set_list)
            if not d["FIRST_IMAGE"]:
                workspace.display_data.wrote_image = False
                self.save_filename_measurements(workspace)
                return False
            d["FIRST_IMAGE"] = False
            
        elif self.when_to_save == WS_LAST_CYCLE:
            workspace.display_data.wrote_image = False
            self.save_filename_measurements( workspace)
            return False
        self.save_image(workspace)
        return True
    
    
    def run_movie(self, workspace):
        out_file = self.get_filename(workspace, check_overwrite=False)
        # overwrite checks are made only for first frame.
        d = self.get_dictionary(workspace.image_set_list)
        if d["CURRENT_FRAME"] == 0 and os.path.exists(out_file):
            if not self.check_overwrite(out_file, workspace):
                d["CURRENT_FRAME"] = "Ignore"
                return
            else:
                # Have to delete the old movie before making the new one
                os.remove(out_file)
        elif d["CURRENT_FRAME"] == "Ignore":
            return
            
        image = workspace.image_set.get_image(self.image_name.value)
        pixels = image.pixel_data
        pixels = pixels * 255
        frames = d['N_FRAMES']
        current_frame = d["CURRENT_FRAME"]
        d["CURRENT_FRAME"] += 1
        self.do_save_image(workspace, out_file, pixels, ome.PT_UINT8,
                           t = current_frame, size_t = frames)
    
    def run_objects(self, workspace):
        objects_name = self.objects_name.value
        objects = workspace.object_set.get_objects(objects_name)
        filename = self.get_filename(workspace)
        if filename is None:  # failed overwrite check
            return

        pixels = objects.segmented
        if self.get_file_format() == FF_MAT:
            scipy.io.matlab.mio.savemat(filename,{"Image":pixels},format='5')
        
        elif self.gray_or_color == GC_GRAYSCALE:
            if objects.count > 255:
                pixel_type = ome.PT_UINT16
            else:
                pixel_type = ome.PT_UINT8
            self.do_save_image(workspace, filename, pixels, pixel_type)
        
        else:
            if self.colormap == cps.DEFAULT:
                colormap = cpp.get_default_colormap()
            else:
                colormap = self.colormap.value
            cm = matplotlib.cm.get_cmap(colormap)
                
            mapper = matplotlib.cm.ScalarMappable(cmap=cm)
            cpixels = mapper.to_rgba(distance_color_labels(pixels), bytes=True)
            cpixels[pixels == 0,:3] = 0
            self.do_save_image(workspace, filename, cpixels, ome.PT_UINT8)
        self.save_filename_measurements(workspace)
        workspace.display_data.wrote_image = True
    
    def post_group(self, workspace, *args):
        if (self.when_to_save == WS_LAST_CYCLE and 
            self.save_image_or_figure != IF_MOVIE):
            self.save_image(workspace)
        
    def do_save_image(self, workspace, filename, pixels, pixel_type, 
                   c = 0, z = 0, t = 0,
                   size_c = 1, size_z = 1, size_t = 1,
                   channel_names = None):
        '''Save image using subimager
        
        workspace - the current workspace
        
        filename - save to this filename
        
        pixels - the image to save
        
        pixel_type - save using this pixel type
        
        c - the image's channel index
        
        z - the image's z index
        
        t - the image's t index
        
        sizeC - # of channels in the stack
        
        sizeZ - # of z stacks
        
        sizeT - # of timepoints in the stack
        
        channel_names - names of the channels (make up names if not present
        '''
        omexml = ome.OMEXML()
        omexml.image(0).Name = os.path.split(filename)[1]
        p = omexml.image(0).Pixels
        assert isinstance(p, ome.OMEXML.Pixels)
        p.SizeX = pixels.shape[1]
        p.SizeY = pixels.shape[0]
        p.SizeC = size_c
        p.SizeT = size_t
        p.SizeZ = size_t
        p.DimensionOrder = ome.DO_XYCZT
        p.PixelType = pixel_type
        index = c + size_c * z + size_c * size_z * t
        if pixels.ndim == 3:
            p.SizeC = pixels.shape[2]
            p.Channel(0).SamplesPerPixel = pixels.shape[2]
            omexml.structured_annotations.add_original_metadata(
                ome.OM_SAMPLES_PER_PIXEL, str(pixels.shape[2]))
        
        url = pathname2url(filename)
        post_image(url, pixels, omexml.to_xml(), index = str(index))

    def save_image(self, workspace):
        workspace.display_data.wrote_image = False
        image = workspace.image_set.get_image(self.image_name.value)
        if self.save_image_or_figure == IF_IMAGE:
            pixels = image.pixel_data
            if self.file_format != FF_MAT:
                if self.rescale.value:
                    pixels = pixels.copy()
                    # Normalize intensities for each channel
                    if pixels.ndim == 3:
                        # RGB
                        for i in range(3):
                            img_min = np.min(pixels[:,:,i])
                            img_max = np.max(pixels[:,:,i])
                            if img_max > img_min:
                                pixels[:,:,i] = (pixels[:,:,i] - img_min) / (img_max - img_min)
                    else:
                        # Grayscale
                        img_min = np.min(pixels)
                        img_max = np.max(pixels)
                        if img_max > img_min:
                            pixels = (pixels - img_min) / (img_max - img_min)
                else:
                    # Clip at 0 and 1
                    if np.max(pixels) > 1 or np.min(pixels) < 0:
                        sys.stderr.write(
                            "Warning, clipping image %s before output. Some intensities are outside of range 0-1" %
                            self.image_name.value)
                        pixels = pixels.copy()
                        pixels[pixels < 0] = 0
                        pixels[pixels > 1] = 1
                        
                if pixels.ndim == 2 and self.colormap != CM_GRAY:
                    # Convert grayscale image to rgb for writing
                    if self.colormap == cps.DEFAULT:
                        colormap = cpp.get_default_colormap()
                    else:
                        colormap = self.colormap.value
                    cm = matplotlib.cm.get_cmap(colormap)
                    
                    if self.get_bit_depth() == '8':
                        mapper = matplotlib.cm.ScalarMappable(cmap=cm)
                        pixels = mapper.to_rgba(pixels, bytes=True)
                        pixel_type = ome.PT_UINT8
                    else:
                        pixel_type = ome.PT_UINT16
                        pixels *= 255
                elif self.get_bit_depth() == '8':
                    pixels = (pixels*255).astype(np.uint8)
                    pixel_type = ome.PT_UINT8
                else:
                    pixels = (pixels*65535)
                    pixel_type = ome.PT_UINT16
                
        elif self.save_image_or_figure == IF_MASK:
            pixels = image.mask.astype(np.uint8) * 255
            pixel_type = ome.PT_BIT
            
        elif self.save_image_or_figure == IF_CROPPING:
            pixels = image.crop_mask.astype(np.uint8) * 255
            pixel_type = ome.PT_BIT

        filename = self.get_filename(workspace)
        if filename is None:  # failed overwrite check
            return

        if self.get_file_format() == FF_MAT:
            scipy.io.matlab.mio.savemat(filename,{"Image":pixels},format='5')
        else:
            self.do_save_image(workspace, filename, pixels, pixel_type)
        workspace.display_data.wrote_image = True
        if self.when_to_save != WS_LAST_CYCLE:
            self.save_filename_measurements(workspace)
        
    def check_overwrite(self, filename, workspace):
        '''Check to see if it's legal to overwrite a file

        Throws an exception if can't overwrite and no interaction available.
        Returns False if can't overwrite, otherwise True.
        '''
        if not self.overwrite.value and os.path.isfile(filename):
            try:
                return (workspace.interaction_request(self, workspace.measurements.image_set_number, filename) == "Yes")
            except workspace.NoInteractionException:
                raise ValueError('SaveImages: trying to overwrite %s in headless mode, but Overwrite files is set to "No"' % (filename))
        return True

    def handle_interaction(self, image_set_number, filename):
        '''handle an interaction request from check_overwrite()'''
        import wx
        dlg = wx.MessageDialog(wx.GetApp().TopWindow,
                               "%s #%d, set #%d - Do you want to overwrite %s?" % \
                                   (self.module_name, self.module_num, image_set_number, filename),
                               "Warning: overwriting file", wx.YES_NO | wx.ICON_QUESTION)
        result = dlg.ShowModal() == wx.ID_YES
        return "Yes" if result else "No"

    def save_filename_measurements(self, workspace):
        if self.update_file_names.value:
            filename = self.get_filename(workspace, make_dirs = False,
                                         check_overwrite = False)
            pn, fn = os.path.split(filename)
            workspace.measurements.add_measurement('Image',
                                                   self.file_name_feature,
                                                   fn,
                                                   can_overwrite=True)
            workspace.measurements.add_measurement('Image',
                                                   self.path_name_feature,
                                                   pn,
                                                   can_overwrite=True)
    
    @property
    def file_name_feature(self):
        '''The file name measurement for the output file'''
        if self.save_image_or_figure == IF_OBJECTS:
            return '_'.join((C_OBJECTS_FILE_NAME, self.objects_name.value))
        return '_'.join((C_FILE_NAME, self.image_name.value))
    
    @property
    def path_name_feature(self):
        '''The path name measurement for the output file'''
        if self.save_image_or_figure == IF_OBJECTS:
            return '_'.join((C_OBJECTS_PATH_NAME, self.objects_name.value))
        return '_'.join((C_PATH_NAME, self.image_name.value))
    
    @property
    def source_file_name_feature(self):
        '''The file name measurement for the exemplar disk image'''
        return '_'.join((C_FILE_NAME, self.file_image_name.value))
    
    def source_path(self, workspace):
        '''The path for the image data, or its first parent with a path'''
        if self.file_name_method.value == FN_FROM_IMAGE:
            path_feature = '%s_%s' % (C_PATH_NAME, self.file_image_name.value)
            assert workspace.measurements.has_feature(cpmeas.IMAGE, path_feature),\
                "Image %s does not have a path!" % (self.file_image_name.value)
            return workspace.measurements.get_current_image_measurement(path_feature)

        # ... otherwise, chase the cpimage hierarchy looking for an image with a path
        cur_image = workspace.image_set.get_image(self.image_name.value)
        while cur_image.path_name is None:
            cur_image = cur_image.parent_image
            assert cur_image is not None, "Could not determine source path for image %s' % (self.image_name.value)"
        return cur_image.path_name
    
    def get_measurement_columns(self, pipeline):
        if self.update_file_names.value:
            return [(cpmeas.IMAGE, 
                     self.file_name_feature,
                     cpmeas.COLTYPE_VARCHAR_FILE_NAME),
                    (cpmeas.IMAGE,
                     self.path_name_feature,
                     cpmeas.COLTYPE_VARCHAR_PATH_NAME)]
        else:
            return []
        
    def get_filename(self, workspace, make_dirs=True, check_overwrite=True):
        "Concoct a filename for the current image based on the user settings"
        
        measurements=workspace.measurements
        if self.file_name_method == FN_SINGLE_NAME:
            filename = self.single_file_name.value
            filename = workspace.measurements.apply_metadata(filename)
        elif self.file_name_method == FN_SEQUENTIAL:
            filename = self.single_file_name.value
            filename = workspace.measurements.apply_metadata(filename)
            padded_num_string = str(measurements.image_set_number).zfill(int(np.ceil(np.log10(workspace.image_set_list.count()+1))))
            filename = '%s%s'%(filename, padded_num_string)
        else:
            file_name_feature = self.source_file_name_feature
            filename = measurements.get_current_measurement('Image',
                                                            file_name_feature)
            filename = os.path.splitext(filename)[0]
            if self.wants_file_name_suffix:
                suffix = self.file_name_suffix.value
                suffix = workspace.measurements.apply_metadata(suffix)
                filename += suffix
        
        filename = "%s.%s"%(filename,self.get_file_format())
        pathname = self.pathname.get_absolute_path(measurements)
        if self.create_subdirectories:
            image_path = self.source_path(workspace)
            subdir = relpath(image_path, cpp.get_default_image_directory())
            pathname = os.path.join(pathname, subdir)
        if len(pathname) and not os.path.isdir(pathname) and make_dirs:
            os.makedirs(pathname)
        result = os.path.join(pathname, filename)
        if check_overwrite and not self.check_overwrite(result, workspace):
            return
        
        return result
    
    def get_file_format(self):
        """Return the file format associated with the extension in self.file_format
        """
        if self.save_image_or_figure == IF_MOVIE:
            return FF_AVI
        if self.file_format == FF_JPG:
            return FF_JPEG
        if self.file_format == FF_TIF:
            return FF_TIFF
        return self.file_format.value
    
    def get_bit_depth(self):
        if (self.save_image_or_figure == IF_IMAGE and 
            self.get_file_format() in FF_SUPPORTING_16_BIT):
            return self.bit_depth.value
        else:
            return '8'
    
    def upgrade_settings(self, setting_values, variable_revision_number, 
                         module_name, from_matlab):
        """Adjust the setting values to be backwards-compatible with old versions
        
        """
        
        PC_DEFAULT     = "Default output folder"

        #################################
        #
        # Matlab legacy
        #
        #################################
        if from_matlab and variable_revision_number == 12:
            # self.create_subdirectories.value is already False by default.
            variable_revision_number = 13
        if from_matlab and variable_revision_number == 13:
            new_setting_values = list(setting_values)
            for i in [3, 12]:
                if setting_values[i] == '\\':
                    new_setting_values[i] == cps.DO_NOT_USE
            variable_revision_number = 14
        if from_matlab and variable_revision_number == 14:
            new_setting_values = []
            if setting_values[0].isdigit():
                new_setting_values.extend([IF_FIGURE,setting_values[1]])
            elif setting_values[3] == 'avi':
                new_setting_values.extend([IF_MOVIE, setting_values[0]])
            elif setting_values[0].startswith("Cropping"):
                new_setting_values.extend([IF_CROPPING, 
                                           setting_values[0][len("Cropping"):]])
            elif setting_values[0].startswith("CropMask"):
                new_setting_values.extend([IF_MASK, 
                                           setting_values[0][len("CropMask"):]])
            else:
                new_setting_values.extend([IF_IMAGE, setting_values[0]])
            new_setting_values.append(new_setting_values[1])
            if setting_values[1] == 'N':
                new_setting_values.extend([FN_SEQUENTIAL,"None","None"])
            elif setting_values[1][0] == '=':
                new_setting_values.extend([FN_SINGLE_NAME,setting_values[1][1:],
                                           setting_values[1][1:]])
            else:
                if len(cpmeas.find_metadata_tokens(setting_values[1])):
                    new_setting_values.extend([FN_WITH_METADATA, setting_values[1],
                                               setting_values[1]])
                else:
                    new_setting_values.extend([FN_FROM_IMAGE, setting_values[1],
                                               setting_values[1]])
            new_setting_values.extend(setting_values[2:4])
            if setting_values[4] == '.':
                new_setting_values.extend([PC_DEFAULT, "None"])
            elif setting_values[4] == '&':
                new_setting_values.extend([PC_WITH_IMAGE, "None"])
            else:
                if len(cpmeas.find_metadata_tokens(setting_values[1])):
                    new_setting_values.extend([PC_WITH_METADATA,
                                               setting_values[4]])
                else:
                    new_setting_values.extend([PC_CUSTOM, setting_values[4]])
            new_setting_values.extend(setting_values[5:11])
            #
            # Last value is there just to display some text in Matlab
            #
            new_setting_values.extend(setting_values[12:-1])
            setting_values = new_setting_values
            from_matlab = False
            variable_revision_number = 1
           
        ##########################
        #
        # Version 1
        #
        ##########################
        if not from_matlab and variable_revision_number == 1:
            # The logic of the question about overwriting was reversed.            
            if setting_values[11] == cps.YES:
                setting_values[11] = cps.NO
            else: 
                setting_values[11] = cps.YES       
            variable_revision_number = 2
            
        #########################
        #
        # Version 2
        #
        #########################
        if (not from_matlab) and variable_revision_number == 2:
            # Default image/output directory -> Default Image Folder
            if setting_values[8].startswith("Default output"):
                setting_values = (setting_values[:8] +
                                  [PC_DEFAULT]+ setting_values[9:])
            elif setting_values[8].startswith("Same"):
                setting_values = (setting_values[:8] +
                                  [PC_WITH_IMAGE] + setting_values[9:])
            variable_revision_number = 3
            
        #########################
        #
        # Version 3
        #
        #########################
        if (not from_matlab) and variable_revision_number == 3:
            # Changed save type from "Figure" to "Module window"
            if setting_values[0] == "Figure":
                setting_values[0] = IF_FIGURE
            setting_values = standardize_default_folder_names(setting_values,8)
            variable_revision_number = 4

        #########################
        #
        # Version 4
        #
        #########################
        if (not from_matlab) and variable_revision_number == 4:
            save_image_or_figure, image_name, figure_name,\
	    file_name_method, file_image_name, \
	    single_file_name, file_name_suffix, file_format, \
	    pathname_choice, pathname, bit_depth, \
	    overwrite, when_to_save, \
            when_to_save_movie, rescale, colormap, \
            update_file_names, create_subdirectories = setting_values

            pathname = SaveImagesDirectoryPath.static_join_string(
                pathname_choice, pathname)
            
            setting_values = [
                save_image_or_figure, image_name, figure_name,
                file_name_method, file_image_name, single_file_name, 
                file_name_suffix != cps.DO_NOT_USE,
                file_name_suffix, file_format,
                pathname, bit_depth, overwrite, when_to_save,
                rescale, colormap, update_file_names, create_subdirectories]
            variable_revision_number = 5
            
        #######################
        #
        # Version 5
        #
        #######################
        if (not from_matlab) and variable_revision_number == 5:
            setting_values = list(setting_values)
            file_name_method = setting_values[3]
            single_file_name = setting_values[5]
            wants_file_suffix = setting_values[6]
            file_name_suffix = setting_values[7]
            if file_name_method == FN_IMAGE_FILENAME_WITH_METADATA:
                file_name_suffix = single_file_name
                wants_file_suffix = cps.YES
                file_name_method = FN_FROM_IMAGE
            elif file_name_method == FN_WITH_METADATA:
                file_name_method = FN_SINGLE_NAME
            setting_values[3] = file_name_method
            setting_values[6] = wants_file_suffix
            setting_values[7] = file_name_suffix
            variable_revision_number = 6
            
        ######################
        #
        # Version 6 - added objects
        #
        ######################
        if (not from_matlab) and (variable_revision_number == 6):
            setting_values = (
                setting_values[:2] + ["None"] + setting_values[2:14] +
                [ GC_GRAYSCALE ] + setting_values[14:])
            variable_revision_number = 7
        setting_values[OFFSET_DIRECTORY_PATH] = \
            SaveImagesDirectoryPath.upgrade_setting(setting_values[OFFSET_DIRECTORY_PATH])
        
        return setting_values, variable_revision_number, from_matlab
    
    def validate_module(self, pipeline):
        if (self.save_image_or_figure == IF_MOVIE and not has_bioformats):
            raise cps.ValidationError("CellProfiler requires bioformats to write movies.",
                                      self.save_image_or_figure)

        if sys.platform == 'darwin':
            if (self.file_format in FF_SUPPORTING_16_BIT and 
                self.save_image_or_figure == IF_IMAGE and
                self.get_bit_depth()== '16' and 
                not has_tiff):
                raise cps.ValidationError("Writing TIFFs on OS X using bioformats may cause CellProfiler to hang or crash (install libtiff & pylibtiff).",
                                          self.bit_depth)
            if (self.save_image_or_figure == IF_MOVIE):
                raise cps.ValidationError("Saving movies on OS X may cause CellProfiler to hang or crash.",
                                          self.save_image_or_figure)

        if (self.save_image_or_figure in (IF_IMAGE, IF_MASK, IF_CROPPING) and
            self.when_to_save in (WS_FIRST_CYCLE, WS_EVERY_CYCLE)):
            #
            # Make sure that the image name is available on every cycle
            #
            for setting in cps.get_name_providers(pipeline,
                                                  self.image_name):
                if setting.provided_attributes.get(cps.AVAILABLE_ON_LAST_ATTRIBUTE):
                    #
                    # If we fell through, then you can only save on the last cycle
                    #
                    raise cps.ValidationError("%s is only available after processing all images in an image group" %
                                              self.image_name.value,
                                              self.when_to_save)

        # XXX - should check that if file_name_method is
        # FN_FROM_IMAGE, that the named image actually has the
        # required path measurement

        # Make sure metadata tags exist
        if self.file_name_method == FN_SINGLE_NAME or \
                (self.file_name_method == FN_FROM_IMAGE and self.wants_file_name_suffix.value):
            text_str = self.single_file_name.value if self.file_name_method == FN_SINGLE_NAME else self.file_name_suffix.value
            undefined_tags = pipeline.get_undefined_metadata_tags(text_str)
            if len(undefined_tags) > 0:
                raise cps.ValidationError("%s is not a defined metadata tag. Check the metadata specifications in your load modules" %
                                     undefined_tags[0], 
                                     self.single_file_name if self.file_name_method == FN_SINGLE_NAME else self.file_name_suffix)
    
class SaveImagesDirectoryPath(cps.DirectoryPath):
    '''A specialized version of DirectoryPath to handle saving in the image dir'''
    
    def __init__(self, text, file_image_name, doc):
        '''Constructor
        text - explanatory text to display
        file_image_name - the file_image_name setting so we can save in same dir
        doc - documentation for user
        '''
        super(SaveImagesDirectoryPath, self).__init__(
            text, dir_choices = [
                cps.DEFAULT_OUTPUT_FOLDER_NAME, cps.DEFAULT_INPUT_FOLDER_NAME,
                PC_WITH_IMAGE, cps.ABSOLUTE_FOLDER_NAME,
                cps.DEFAULT_OUTPUT_SUBFOLDER_NAME, 
                cps.DEFAULT_INPUT_SUBFOLDER_NAME], doc=doc)
        self.file_image_name = file_image_name
        
    def get_absolute_path(self, measurements=None, image_set_index=None):
        if self.dir_choice == PC_WITH_IMAGE:
            path_name_feature = "PathName_%s" % self.file_image_name.value
            return measurements.get_current_image_measurement(path_name_feature)
        return super(SaveImagesDirectoryPath, self).get_absolute_path(
            measurements, image_set_index)
    
    def test_valid(self, pipeline):
        if self.dir_choice not in self.dir_choices:
            raise cps.ValidationError("%s is not a valid directory option" %
                                      self.dir_choice, self)
        
    @staticmethod
    def upgrade_setting(value):
        '''Upgrade setting from previous version'''
        dir_choice, custom_path = cps.DirectoryPath.split_string(value)
        if dir_choice in OLD_PC_WITH_IMAGE_VALUES:
            dir_choice = PC_WITH_IMAGE
        elif dir_choice in (PC_CUSTOM, PC_WITH_METADATA):
            if custom_path.startswith('.'):
                dir_choice = cps.DEFAULT_OUTPUT_SUBFOLDER_NAME
            elif custom_path.startswith('&'):
                dir_choice = cps.DEFAULT_INPUT_SUBFOLDER_NAME
                custom_path = '.' + custom_path[1:]
            else:
                dir_choice = cps.ABSOLUTE_FOLDER_NAME
        else:
            return cps.DirectoryPath.upgrade_setting(value)
        return cps.DirectoryPath.static_join_string(dir_choice, custom_path)
                  
if __name__=="__main__":
    from subimager.client import start_subimager, stop_subimager
    start_subimager()
    i, j = np.mgrid[0:100, 0:100]
    img = np.zeros((100,100,3))
    img[np.sqrt((i - 50)**2 + (j-50)**2) < 25, 0] = 255
    img[np.sqrt((i - 25)**2 + (j-25)**2) < 25, 1] = 255
    img[np.sqrt((i - 25)**2 + (j-75)**2) < 25, 2] = 255
    s = SaveImages()
    s.do_save_image(None, "c:\\temp\\image.tif", img, "uint8")
    stop_subimager()
