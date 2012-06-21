'''<b>RunImageJ</b> runs an ImageJ command.
<hr>

<a href="http://rsbweb.nih.gov/ij/">ImageJ</a> is an image processing and analysis program.
It operates by processing commands that operate on one or more images,
possibly modifying the images. ImageJ has a macro language which can
be used to program its operation and customize its operation, similar to
CellProfiler pipelines. ImageJ maintains a current image and most commands
operate on this image, but it's possible to load multiple images into
ImageJ and operate on them together.

The <b>RunImageJ</b> module runs one ImageJ command or macro per cycle. It first
loads the images you want to process into ImageJ, then runs the command, and, if
desired, retrieves images you want to process further in CellProfiler.'''

__version__ = "$Revision$"

import logging
logger = logging.getLogger(__name__)
import numpy as np
import sys
import uuid

import cellprofiler.cpmodule as cpm
import cellprofiler.cpimage as cpi
import cellprofiler.settings as cps
import cellprofiler.preferences as cpprefs
from cellprofiler.gui.help import BATCH_PROCESSING_HELP_REF
import imagej.ijbridge as ijb
import subimager.imagejrequest as ijrq

CM_COMMAND = "Command"
CM_MACRO = "Macro"
CM_NOTHING = "Nothing"

D_FIRST_IMAGE_SET = "FirstImageSet"
D_LAST_IMAGE_SET = "LastImageSet"

cached_commands = None
cached_choice_tree = None

'''The index of the imageJ command in the settings'''
IDX_COMMAND_CHOICE = 0
IDX_COMMAND = 1
IDX_PRE_COMMAND_CHOICE = 9
IDX_PRE_COMMAND = 10
IDX_POST_COMMAND_CHOICE = 13
IDX_POST_COMMAND = 14

TYPE_BOOLEAN = ("boolean", "java.lang.Boolean")
TYPE_INTEGER = ("int", "java.lang.Integer", "long", "java.lang.Long",
                "short", "java.lang.Short", "byte", "java.lang.Byte")
TYPE_FLOAT = ("float", "java.lang.Float", "double", "java.lang.Double")
TYPE_STRING = ("java.lang.String")
TYPE_IMAGE = ("imagej.data.dataset", "imagej.data.display.ImageDisplay")
TYPE_COLOR = ("imagej.util.ColorRGB", "imagej.util.ColorRGBA")
TYPE_FILE = ("java.io.File")

'''ImageJ images are scaled from 0 to 255'''
IMAGEJ_SCALE = 255.0

class RunImageJ(cpm.CPModule):
    module_name = "RunImageJ"
    variable_revision_number = 3
    category = "Image Processing"
    
    def create_settings(self):
        '''Create the settings for the module'''
        self.command_or_macro = cps.Choice(
            "Run an ImageJ command or macro?", [CM_COMMAND, CM_MACRO],
            doc = """This setting determines whether <b>RunImageJ</b> runs either a:
            <ul>
            <li><i>Command:</i> Select from a list of available ImageJ commands
            (those items contained in the ImageJ menus); or</li>
            <li><i>Macro:</i> A series of ImageJ commands/plugins that you write yourself.</li>
            </ul>""")
        #
        # Load the commands in visible_settings so that we don't call
        # ImageJ unless someone tries the module
        #
        self.command = self.make_command_choice(
            "Command",
            doc = """<i>(Used only if running a command)</i><br>
            The command to execute when the module runs.""")
                                                
        self.command_settings_dictionary = {}
        self.command_settings = []
        self.command_settings_count = cps.HiddenCount(
            self.command_settings, "Command settings count")
        self.pre_command_settings_dictionary = {}
        self.pre_command_settings = []
        self.pre_command_settings_count = cps.HiddenCount(
            self.pre_command_settings, "Prepare group command settings count")
        self.post_command_settings_dictionary = {}
        self.post_command_settings = []
        self.post_command_settings_count = cps.HiddenCount(
            self.post_command_settings, "Post-group command settings count")

        self.macro = cps.Text(
            "Macro", 'run("Invert");',
            multiline = True,
            doc="""<i>(Used only if running a macro)</i><br>
            This is the ImageJ macro to be executed. For help on
            writing macros, see <a href="http://rsb.info.nih.gov/ij/developer/macro/macros.html">here</a>.""")
        
        self.options = cps.Text(
            "Options", "",
            doc = """<i>(Used only if running a command)</i><br>
            Use this setting to provide options to the command.""")

        self.wants_to_set_current_image = cps.Binary(
            "Input the currently active image in ImageJ?", True,
            doc="""<p>Check this setting if you want to set the currently 
            active ImageJ image using an image from a 
            prior CellProfiler module.</p>
            <p>Leave it unchecked to use the currently 
            active image in ImageJ. You may want to do this if you
            have an output image from a prior <b>RunImageJ</b>
            that you want to perform further operations upon
            before retrieving the final result back to CellProfiler.</p>""")

        self.current_input_image_name = cps.ImageNameSubscriber(
            "Select the input image",
            doc="""<i>(Used only if setting the currently active image)</i><br>
            This is the CellProfiler image that will become 
            ImageJ's currently active image.
            The ImageJ commands and macros in this module will perform 
            their operations on this image. You may choose any image produced
            by a prior CellProfiler module.""")

        self.wants_to_get_current_image = cps.Binary(
            "Retrieve the currently active image from ImageJ?", True,
            doc="""Check this setting if you want to retrieve ImageJ's
            currently active image after running the command or macro. 
            <p>Leave
            the setting unchecked if the pipeline does not need to access
            the current ImageJ image. For example, you might want to run
            further ImageJ operations with additional <b>RunImageJ</b>
            upon the current image
            prior to retrieving the final image back to CellProfiler.</p>""")

        self.current_output_image_name = cps.ImageNameProvider(
            "Name the current output image", "ImageJImage",
            doc="""<i>(Used only if retrieving the currently active image)</i><br>
            This is the CellProfiler name for ImageJ's current image after
            processing by the command or macro. The image will be a
            snapshot of the current image after the command has run, and
            will be avilable for processing by subsequent CellProfiler modules.""")
        
        self.pause_before_proceeding = cps.Binary(
            "Wait for ImageJ before continuing?", False,
            doc = """Some ImageJ commands and macros are interactive; you
            may want to adjust the image in ImageJ before continuing. Check
            this box to stop CellProfiler while you adjust the image in
            ImageJ. Leave the box unchecked to immediately use the image.
            <br>
            This command will not wait if CellProfiler is executed in
            batch mode. See <i>%(BATCH_PROCESSING_HELP_REF)s</i> for more
            details on batch processing."""%globals())
        
        self.prepare_group_choice = cps.Choice(
            "Run a command or macro before each group of images?", [CM_NOTHING, CM_COMMAND, CM_MACRO],
            doc="""You can run an ImageJ macro or a command <i>before</i> each group of
            images. This can be useful in order to set up ImageJ before
            processing a stack of images. Choose <i>%(CM_NOTHING)s</i> if
            you do not want to run a command or macro, <i>%(CM_COMMAND)s</i>
            to choose a command to run or <i>%(CM_MACRO)s</i> to run a macro.
            """ % globals())
        
        self.prepare_group_command = self.make_command_choice(
            "Command", 
            doc = """<i>(Used only if running a command before an image group)</i><br>
            The command to execute before processing a group of images.""")

        self.prepare_group_macro = cps.Text(
            "Macro", 'run("Invert");',
            multiline = True,
            doc="""<i>(Used only if running a macro before an image group)</i><br>
            This is the ImageJ macro to be executed before processing
            a group of images. For help on writing macros, see 
            <a href="http://rsb.info.nih.gov/ij/developer/macro/macros.html">here</a>.""")
        
        self.prepare_group_options = cps.Text(
            "Options", "",
            doc = """<i>(Used only if running a command before an image group)</i><br>
            Use this setting to provide options to the command.""")
        
        self.post_group_choice = cps.Choice(
            "Run a command or macro after each group of images?", [CM_NOTHING, CM_COMMAND, CM_MACRO],
            doc="""You can run an ImageJ macro or a command <i>after</i> each group of
            images. This can be used to do some sort of operation on a whole
            stack of images that have been accumulated by the group operation.
            Choose <i>%(CM_NOTHING)s</i> if you do not want to run a command or 
            macro, <i>%(CM_COMMAND)s</i> to choose a command to run or 
            <i>%(CM_MACRO)s</i> to run a macro.
            """ % globals())
        
        self.post_group_command = self.make_command_choice(
            "Command", 
            doc = """
            <i>(Used only if running a command after an image group)</i><br>
            The command to execute after processing a group of images.""")
        
        self.post_group_macro = cps.Text(
            "Macro", 'run("Invert");',
            multiline = True,
            doc="""<i>(Used only if running a macro after an image group)</i><br>
            This is the ImageJ macro to be executed after processing
            a group of images. For help on writing macros, see 
            <a href="http://rsb.info.nih.gov/ij/developer/macro/macros.html">here</a>.""")
        
        self.post_group_options = cps.Text(
            "Options", "",
            doc = """<i>(Used only if running a command after an image group)</i><br>
            Use this setting to provide options to the command or
            macro.""")
        
        self.wants_post_group_image = cps.Binary(
            "Retrieve the image output by the group operation?", False,
            doc="""You can retrieve the image that is currently active in ImageJ
            at the end of macro processing and use it later in CellProfiler.
            The image will only be available during the last cycle of the
            image group. Check this setting to use the active image in CellProfiler
            or leave it unchecked if you do not want to use the active image.
            """)
        
        self.post_group_output_image = cps.ImageNameProvider(
            "Name the group output image", "ImageJGroupImage",
            doc="""<i>(Used only if retrieving an image after an image group operation)</i><br>
            This setting names the output image produced by the
            ImageJ command or macro that CellProfiler runs after processing
            all images in the group. The image is only available at the
            last cycle in the group""",
            provided_attributes={cps.AGGREGATE_IMAGE_ATTRIBUTE: True,
                                 cps.AVAILABLE_ON_LAST_ATTRIBUTE: True } )
           
        self.show_imagej_button = cps.DoSomething(
            "Show ImageJ", "Show", self.on_show_imagej,
            doc="""Press this button to show the ImageJ user interface.
            You can use the user interface to run ImageJ commands or
            set up ImageJ before a CellProfiler run.""")
        
    def make_command_choice(self, label, doc):
        '''Make a version-appropriate command chooser setting
        
        label - the label text for the setting
        doc - its documentation
        '''
        return cps.TreeChoice(label, "None", self.get_choice_tree, doc = doc)
        
    def get_command_choices(self, pipeline):
        return sorted(self.get_cached_commands().keys())
    
    def get_choice_tree(self):
        '''Get the ImageJ command choices for the TreeChoice control
        
        The menu items are augmented with a third tuple entry which is
        the ModuleInfo for the command.
        '''
        global cached_choice_tree
        global cached_commands
        if cached_choice_tree is not None:
            return cached_choice_tree
        tree = []
        
        for module_info in ijb.get_ij_bridge().get_commands():
            assert isinstance(module_info, ijrq.ModuleInfoType)
            cached_commands
            if module_info.MenuRoot != "app":
                continue
            menu_path = module_info.MenuPath
            if menu_path is None or len(menu_path.menu_entry) == 0:
                continue
            current_tree = tree
            for item in menu_path.menu_entry:
                assert isinstance(item, ijrq.MenuEntryType)
                name = item.Name
                weight = item.Weight
                matches = [node for node in current_tree
                           if node[0] == name]
                if len(matches) > 0:
                    current_node = matches[0]
                else:
                    current_node = [name, [], module_info, weight]
                    current_tree.append(current_node)
                current_tree = current_node[1]
            # mark the leaf.
            current_node[1] = None
            
        def sort_tree(tree):
            '''Recursively sort a tree in-place'''
            for node in tree:
                if node[1] is not None:
                    sort_tree(node[1])
            tree.sort(lambda node1, node2: cmp(node1[-1], node2[-1]))
        sort_tree(tree)
        cached_choice_tree = tree
        return cached_choice_tree
        
    def get_command_settings(self, command, d):
        '''Get the settings associated with the current command
        
        d - the dictionary that persists the setting. None = regular
        '''
        key = command.get_unicode_value()
        if not d.has_key(key):
            try:
                module_info = command.get_selected_leaf()[2]
            except cps.ValidationError:
                return []
            result = []
            assert isinstance(module_info, ijrq.ModuleInfoType)
            inputs = module_info.Input
            implied_outputs = []
            for module_item in inputs:
                assert isinstance(module_item, ijrq.ModuleItemType)
                field_type = module_item.Type
                label = module_item.Label
                if label is None:
                    label = module_item.Name
                minimum = module_item.MinimumValue
                maximum = module_item.MaximumValue
                description = module_item.Description
                if field_type in TYPE_BOOLEAN:
                    setting = cps.Binary(
                        label,
                        doc = description)
                elif field_type in TYPE_INTEGER:
                    setting = cps.Integer(
                        label,
                        minval if minval is not None else
                        maxval if maxval is not None else 0,
                        minval = minimum,
                        maxval = maximum,
                        doc = description)
                elif field_type in TYPE_FLOAT:
                    setting = cps.Float(
                        label,
                        minval if minval is not None else
                        maxval if maxval is not None else 0,
                        minval = minimum,
                        maxval = maximum,
                        doc = description)
                elif field_type in TYPE_STRING:
                    choices = module_item.Choices
                    value = J.to_string(value)
                    if choices is not None and len(choices) > 0:
                        choices = [J.to_string(choice) 
                                   for choice 
                                   in J.iterate_collection(choices)]
                        setting = cps.Choice(
                            label, choices, value, doc = description)
                    else:
                        setting = cps.Text(
                            label, value, doc = description)
                elif field_type in TYPE_COLOR:
                    value = "#ffffff"
                    setting = cps.Color(label, value, doc = description)
                elif field_type in TYPE_IMAGE:
                    setting = cps.ImageNameSubscriber(
                        label, "InputImage",
                        doc = description)
                    #
                    # This is a Display for ij2 - the plugin typically
                    # scribbles all over the display's image. So
                    # we list it as an output too.
                    #
                    implied_outputs.append((
                        cps.ImageNameProvider(
                            label, "OutputImage",
                            doc = description), module_item))
                elif field_type in TYPE_FILE:
                    setting = cps.FilenameText(
                        label, None, doc = description)
                else:
                    continue
                result.append((setting, module_item))
            for output in module_info.Output:
                field_type = output.Type
                if field_type in TYPE_IMAGE:
                    result.append((cps.ImageNameProvider(
                        label, "ImageJImage",
                        doc = description), output))
            result += implied_outputs
            d[key] = result
        else:
            result = d[key]
        return [setting for setting, module_info in result]
        
    def is_advanced(self, command, d):
        '''A command is an advanced command if there are settings for it'''
        return True
    
    def settings(self):
        '''The settings as loaded or stored in the pipeline'''
        return ([
            self.command_or_macro, self.command, self.macro,
            self.options, self.wants_to_set_current_image,
            self.current_input_image_name,
            self.wants_to_get_current_image, self.current_output_image_name,
            self.pause_before_proceeding,
            self.prepare_group_choice, self.prepare_group_command,
            self.prepare_group_macro, self.prepare_group_options,
            self.post_group_choice, self.post_group_command,
            self.post_group_macro, self.post_group_options,
            self.wants_post_group_image, self.post_group_output_image,
            self.command_settings_count, self.pre_command_settings_count,
            self.post_command_settings_count] + self.command_settings +
                self.pre_command_settings + self.post_command_settings)
    
    def on_setting_changed(self, setting, pipeline):
        '''Respond to a setting change
        
        We have to update the ImageJ module settings in response to a
        new choice.
        '''
        for command_setting, module_settings, d in (
            (self.command, self.command_settings, self.command_settings_dictionary),
            (self.prepare_group_command, self.pre_command_settings, self.pre_command_settings_dictionary),
            (self.post_group_command, self.post_command_settings, self.post_command_settings_dictionary)):
            if id(setting) == id(command_setting):
                del module_settings[:]
                module_settings.extend(self.get_command_settings(setting, d))
                
    def visible_settings(self):
        '''The settings as seen by the user'''
        result = [self.command_or_macro]
        if self.command_or_macro == CM_COMMAND:
            result += [self.command]
            result += self.command_settings
        else:
            result += [self.macro]
        result += [self.wants_to_set_current_image]
        if self.wants_to_set_current_image:
            result += [self.current_input_image_name]
        result += [self.wants_to_get_current_image]
        if self.wants_to_get_current_image:
            result += [self.current_output_image_name]
        result += [ self.prepare_group_choice]
        if self.prepare_group_choice == CM_MACRO:
            result += [self.prepare_group_macro]
        elif self.prepare_group_choice == CM_COMMAND:
            result += [self.prepare_group_command]
            result += self.pre_command_settings
        result += [self.post_group_choice]
        if self.post_group_choice == CM_MACRO:
            result += [self.post_group_macro]
        elif self.post_group_choice == CM_COMMAND:
            result += [self.post_group_command]
            result += self.post_command_settings
        if self.post_group_choice != CM_NOTHING:
            result += [self.wants_post_group_image]
            if self.wants_post_group_image:
                result += [self.post_group_output_image]
        result += [self.pause_before_proceeding, self.show_imagej_button]
        return result
    
    def on_show_imagej(self):
        '''Show the ImageJ user interface
        
        This method shows the ImageJ user interface when the user presses
        the Show ImageJ button.
        '''
        ijb.get_ij_bridge().show_imagej()
        
    def prepare_group(self, workspace, grouping, image_numbers):
        '''Prepare to run a group
        
        RunImageJ remembers the image number of the first and last image
        for later processing.
        '''
        d = self.get_dictionary(workspace.image_set_list)
        d[D_FIRST_IMAGE_SET] = image_numbers[0]
        d[D_LAST_IMAGE_SET] = image_numbers[-1]
        
    def run(self, workspace):
        '''Run the imageJ command'''
        bridge = ijb.get_ij_bridge()
        image_set = workspace.image_set
        d = self.get_dictionary(workspace.image_set_list)
        if self.wants_to_set_current_image:
            input_image_name = self.current_input_image_name.value
            img = image_set.get_image(input_image_name,
                                      must_be_grayscale = True)
        else:
            img = None
        
        #
        # Run a command or macro on the first image of the set
        #
        if d[D_FIRST_IMAGE_SET] == image_set.image_number:
            self.do_imagej(bridge, workspace, D_FIRST_IMAGE_SET)
        #
        # Install the input image as the current image
        #
        if img is not None:
            bridge.inject_image(img.pixel_data * IMAGEJ_SCALE, input_image_name)

        self.do_imagej(bridge, workspace)
        #
        # Get the output image
        #
        if self.wants_to_get_current_image:
            output_image_name = self.current_output_image_name.value
            pixel_data = bridge.get_current_image() / IMAGEJ_SCALE
            image = cpi.Image(pixel_data)
            image_set.add(output_image_name, image)
        #
        # Execute the post-group macro or command
        #
        if d[D_LAST_IMAGE_SET] == image_set.image_number:
            self.do_imagej(bridge, workspace, D_LAST_IMAGE_SET)
            #
            # Save the current ImageJ image after executing the post-group
            # command or macro
            #
            if (self.post_group_choice != CM_NOTHING and
                self.wants_post_group_image):
                output_image_name = self.post_group_output_image.value
                pixel_data = ijb.get_current_image()
                image = cpi.Image(pixel_data)
                image_set.add(output_image_name, image)

    def do_imagej(self, bridge, workspace, when=None):
        if when == D_FIRST_IMAGE_SET:
            choice = self.prepare_group_choice.value
            command = self.prepare_group_command
            macro = self.prepare_group_macro.value
            options = self.prepare_group_options.value
            d = self.pre_command_settings_dictionary
        elif when == D_LAST_IMAGE_SET:
            choice = self.post_group_choice.value
            command = self.post_group_command
            macro = self.post_group_macro.value
            options = self.post_group_options.value
            d = self.pre_command_settings_dictionary
        else:
            choice = self.command_or_macro.value
            command = self.command
            macro  = self.macro.value
            options = self.options.value
            d = self.command_settings_dictionary
            
        if choice == CM_COMMAND:
            self.execute_advanced_command(workspace, command, d)
        elif choice == CM_MACRO:
            macro = workspace.measurements.apply_metadata(macro)
            bridge.execute_macro(macro)
        if (choice != CM_NOTHING and 
            (not cpprefs.get_headless()) and 
            self.pause_before_proceeding):
            import wx
            wx.MessageBox("Please edit the image in ImageJ and hit OK to proceed",
                          "Waiting for ImageJ")
    
    def execute_advanced_command(self, workspace, command, d):
        '''Execute an advanced command

        command - name of the command
        d - dictionary to be used to find settings
        '''
        from subimager.client import make_imagej_request
        self.get_command_settings(command, d)
        wants_display = self.show_window
        if wants_display:
            workspace.display_data.input_images = input_images = []
            workspace.display_data.output_images = output_images = []
        key = command.get_unicode_value()
        node = command.get_selected_leaf()
        module_info = node[2]
        assert isinstance(module_info, ijrq.ModuleInfoType)
        
        run_module_request = ijrq.RunModuleRequestType(
            ContextID = ijb.get_ij_bridge().context_id,
            ModuleID = module_info.ModuleID)
        image_dictionary = {}
        for setting, module_item in d[key]:
            assert isinstance(module_item, ijrq.ModuleItemType)
            field_type = module_item.Type
            if isinstance(setting, cps.ImageNameProvider):
                continue
            parameter = ijrq.ParameterValueType(
                Name = module_item.Name)
            if field_type in TYPE_BOOLEAN:
                parameter.BooleanValue = setting.value
            elif field_type in TYPE_INTEGER:
                parameter.NumberValue = setting.value
            elif field_type in TYPE_FLOAT:
                parameter.NumberValue = setting.value
            elif field_type in TYPE_STRING:
                parameter.StringValue = setting.value
            elif field_type in TYPE_COLOR:
                assert isinstance(setting, cps.Color)
                red, green, blue = setting.to_rgb()
                parameter.ColorValue = ijrq.ColorType(
                    Red = red, Green = green, Blue = blue)
            elif field_type in TYPE_IMAGE:
                image_name = setting.value
                image = workspace.image_set.get_image(image_name)
                pixel_data = image.pixel_data * IMAGEJ_SCALE
                image_dictionary[image_name] = pixel_data
                axis = ["X", "Y"] if pixel_data.ndim == 2 else \
                    ["X", "Y", "CHANNEL"]
                parameter.ImageValue = ijrq.ImageDisplayParameterValueType(
                    ImageName = image_name,
                    ImageID = image_name,
                    Axis = axis)
                if image.has_mask:
                    overlay_name = "X" + uuid.uuid4().get_hex()
                    image_dictionary[overlay_name] = image.mask
                    parameter.ImageValue.Overlay = overlay_name
                if wants_display:
                    input_images.append((image_name, image.pixel_data))
            elif field_type in TYPE_FILE:
                parameter.StringValue = setting.value
            run_module_request.add_Parameter(parameter)
        request = ijrq.RequestType(RunModule = run_module_request)
        response_xml, image_dictionary = \
            make_imagej_request(request, image_dictionary)
        response = ijrq.parseString(response_xml)
        assert isinstance(response, ijrq.ResponseType)
        if response.Exception is not None:
            exception = response.Exception
            assert isinstance(exception, ijrq.ExceptionResponseType)
            logger.warn(exception.Message)
            logger.warn(exception.StackTrace)
            raise Exception(exception.Message)
        parameters = response.RunModuleResponse.Parameter
        for setting, module_item in d[key]:
            if isinstance(setting, cps.ImageNameProvider):
                matching_parameters = [
                    p for p in parameters
                    if p.Name == module_item.Name]
                if len(matching_parameters) == 0:
                    raise Exception("ImageJ call failed to set parameter, %s" %
                                    module_item.Name)
                parameter = matching_parameters[0]
                output_name = setting.value
                pixel_data = image_dictionary[parameter.ImageValue.ImageID]
                pixel_data /= IMAGEJ_SCALE
                image = cpi.Image(pixel_data)
                workspace.image_set.add(output_name, image)
                if wants_display:
                    output_images.append((output_name, pixel_data))
                
    def display(self, workspace, figure):
        if (self.command_or_macro == CM_COMMAND and 
              self.is_advanced(self.command,
                               self.command_settings_dictionary)):
            input_images = workspace.display_data.input_images
            output_images = workspace.display_data.output_images
            primary = None
            if len(input_images) == 0:
                if len(output_images) == 0:
                    figure.figure.text(.25, .5, "No input image",
                                       verticalalignment='center',
                                       horizontalalignment='center')
                    return
                else:
                    nrows = 1
                    output_images = [ 
                        (name, img, i, 0) 
                        for i, (name, img) in enumerate(output_images)]
                    ncols = len(output_images)
            else:
                input_images = [ 
                    (name, img, i, 0) 
                    for i, (name, img) in enumerate(input_images)]
                ncols = len(input_images)
                if len(output_images) == 0:
                    nrows = 1
                else:
                    nrows = 2
                    output_images = [ 
                        (name, img, i, 1) 
                        for i, (name, img) in enumerate(output_images)]
                    ncols = max(ncols, len(output_images))
            figure.set_subplots((ncols, nrows))
            for title, pixel_data, x, y in input_images + output_images:
                if pixel_data.ndim == 3:
                    mimg = figure.subplot_imshow_color(x, y, pixel_data, 
                                                       title=title, 
                                                       sharex = primary,
                                                       sharey = primary)
                else:
                    mimg = figure.subplot_imshow_bw(x, y, pixel_data, 
                                                    title=title,
                                                    sharex = primary,
                                                    sharey = primary)
                if primary is None:
                    primary = mimg
            return
        figure.set_subplots((2, 1))
        if self.wants_to_set_current_image:
            input_image_name = self.current_input_image_name.value
            img = workspace.image_set.get_image(input_image_name)
            pixel_data = img.pixel_data
            title = "Input image: %s" % input_image_name
            if pixel_data.ndim == 3:
                figure.subplot_imshow_color(0,0, pixel_data, title=title)
            else:
                figure.subplot_imshow_bw(0,0, pixel_data, title=title)
        else:
            figure.figure.text(.25, .5, "No input image",
                               verticalalignment='center',
                               horizontalalignment='center')
        
        if self.wants_to_get_current_image:
            output_image_name = self.current_output_image_name.value
            img = workspace.image_set.get_image(output_image_name)
            pixel_data = img.pixel_data
            title = "Output image: %s" % output_image_name
            if pixel_data.ndim == 3:
                figure.subplot_imshow_color(1,0, pixel_data, title=title,
                                            sharex = figure.subplot(0,0),
                                            sharey = figure.subplot(0,0))
            else:
                figure.subplot_imshow_bw(1,0, pixel_data, title=title,
                                         sharex = figure.subplot(0,0),
                                         sharey = figure.subplot(0,0))
        else:
            figure.figure.text(.75, .5, "No output image",
                               verticalalignment='center',
                               horizontalalignment='center')

    def prepare_settings(self, setting_values):
        '''Prepare the settings for loading
        
        set up the advanced settings for the commands
        '''
        for command_settings, idx_choice, idx_cmd, d in (
            (self.command_settings, IDX_COMMAND_CHOICE, IDX_COMMAND, 
             self.command_settings_dictionary),
            (self.pre_command_settings, IDX_PRE_COMMAND_CHOICE, IDX_PRE_COMMAND, 
             self.pre_command_settings_dictionary),
            (self.post_command_settings, IDX_POST_COMMAND_CHOICE, 
             IDX_POST_COMMAND, self.post_command_settings_dictionary)):
            del command_settings[:]
            if setting_values[idx_choice] == CM_COMMAND:
                command = self.make_command_choice("", "")
                command.set_value_text(setting_values[idx_cmd])
                command_settings += self.get_command_settings(
                    command, d)
        
            
    def upgrade_settings(self, setting_values, variable_revision_number,
                         module_name, from_matlab):
        if variable_revision_number == 1:
            setting_values = setting_values + [
                CM_NOTHING, "None",
                'print("Enter macro here")\n', "",
                CM_NOTHING, "None",
                'print("Enter macro here")\n', "",
                cps.NO, "AggregateImage"]
            variable_revision_number = 2
        if variable_revision_number == 2:
            # Added advanced commands
            setting_values = setting_values + ['0','0','0']
            variable_revision_number = 3
        return setting_values, variable_revision_number, from_matlab
        
            
