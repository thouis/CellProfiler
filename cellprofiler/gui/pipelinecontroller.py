"""PipelineController.py - controls (modifies) a pipeline

CellProfiler is distributed under the GNU General Public License.
See the accompanying file LICENSE for details.

Copyright (c) 2003-2009 Massachusetts Institute of Technology
Copyright (c) 2009-2012 Broad Institute
All rights reserved.

Please see the AUTHORS file for credits.

Website: http://www.cellprofiler.org
"""

import csv
import logging
import math
import numpy
import wx
import os
import re
import shutil
import sys
import Queue
import cpframe
import random
import string
import hashlib
from cStringIO import StringIO

import cellprofiler.pipeline as cpp
import cellprofiler.preferences as cpprefs
import cellprofiler.cpimage as cpi
import cellprofiler.measurements as cpm
import cellprofiler.workspace as cpw
import cellprofiler.objects as cpo
from cellprofiler.gui.addmoduleframe import AddModuleFrame
import cellprofiler.gui.moduleview
from cellprofiler.gui.movieslider import EVT_TAKE_STEP
from cellprofiler.gui.help import HELP_ON_MODULE_BUT_NONE_SELECTED
import cellprofiler.utilities.version as version
from errordialog import display_error_dialog, ED_CONTINUE, ED_STOP, ED_SKIP
from runmultiplepipelinesdialog import RunMultplePipelinesDialog
from cellprofiler.modules.loadimages import C_FILE_NAME, C_PATH_NAME, C_FRAME
import cellprofiler.gui.parametersampleframe as psf
import cellprofiler.analysis as cpanalysis
import cellprofiler.cpmodule as cpmodule

logger = logging.getLogger(__name__)
RECENT_FILE_MENU_ID = [wx.NewId() for i in range(cpprefs.RECENT_FILE_COUNT)]
WRITING_MAT_FILE = "Writing .MAT measurements file..."
WROTE_MAT_FILE = ".MAT measurements file has been saved"

class PipelineController:
    """Controls the pipeline through the UI
    
    """
    def __init__(self, workspace, frame):
        self.__workspace = workspace
        pipeline = self.__pipeline = workspace.pipeline
        pipeline.add_listener(self.__on_pipeline_event)
        self.__frame = frame
        self.__add_module_frame = AddModuleFrame(frame,-1,"Add modules")
        self.__add_module_frame.add_listener(self.on_add_to_pipeline)
        # ~*~
        self.__parameter_sample_frame = None
        # ~^~
        self.__setting_errors = {}
        self.__running_pipeline = None
        self.__dirty_pipeline = False
        self.__inside_running_pipeline = False 
        self.__pause_pipeline = False
        self.__pipeline_measurements = None
        self.__debug_image_set_list = None
        self.__debug_measurements = None
        self.__debug_grids = None
        self.__keys = None
        self.__groupings = None
        self.__grouping_index = None
        self.__within_group_index = None
        self.__plate_viewer = None
        self.pipeline_list = []
        cpprefs.add_image_directory_listener(self.__on_image_directory_change)
        cpprefs.add_output_directory_listener(self.__on_output_directory_change)

        # interaction/display requests and exceptions from an Analysis
        self.interaction_request_queue = Queue.PriorityQueue()
        self.interaction_pending = False

        self.populate_recent_files()
        self.menu_id_to_module_name = {}
        self.module_name_to_menu_id = {}
        self.populate_edit_menu(self.__frame.menu_edit_add_module)
        assert isinstance(frame, wx.Frame)
        frame.Bind(wx.EVT_MENU, self.__on_open_workspace, 
                   id = cpframe.ID_FILE_OPEN_WORKSPACE)
        frame.Bind(wx.EVT_MENU, self.__on_new_workspace,
                   id = cpframe.ID_FILE_NEW_WORKSPACE)
        frame.Bind(wx.EVT_MENU, self.__on_save_as_workspace,
                   id = cpframe.ID_FILE_SAVE_AS_WORKSPACE)
        wx.EVT_MENU(frame, cpframe.ID_FILE_LOAD_PIPELINE,self.__on_load_pipeline)
        wx.EVT_MENU(frame, cpframe.ID_FILE_URL_LOAD_PIPELINE, self.__on_url_load_pipeline)
        wx.EVT_MENU(frame, cpframe.ID_FILE_SAVE_PIPELINE,self.__on_save_pipeline)
        wx.EVT_MENU(frame, cpframe.ID_FILE_SAVE_AS_PIPELINE, self.__on_save_as_pipeline)
        wx.EVT_MENU(frame, cpframe.ID_FILE_CLEAR_PIPELINE,self.__on_clear_pipeline)
        wx.EVT_MENU(frame, cpframe.ID_FILE_EXPORT_IMAGE_SETS, self.__on_export_image_sets)
        wx.EVT_MENU(frame, cpframe.ID_FILE_PLATEVIEWER, self.__on_plateviewer)
        wx.EVT_MENU(frame, cpframe.ID_FILE_ANALYZE_IMAGES,self.on_analyze_images)
        wx.EVT_MENU(frame, cpframe.ID_FILE_STOP_ANALYSIS,self.on_stop_running)
        wx.EVT_MENU(frame, cpframe.ID_FILE_RUN_MULTIPLE_PIPELINES, self.on_run_multiple_pipelines)
        wx.EVT_MENU(frame, cpframe.ID_FILE_RESTART, self.on_restart)
        
        wx.EVT_MENU(frame, cpframe.ID_EDIT_MOVE_UP, self.on_module_up)
        wx.EVT_MENU(frame, cpframe.ID_EDIT_MOVE_DOWN, self.on_module_down)
        wx.EVT_MENU(frame, cpframe.ID_EDIT_UNDO, self.on_undo)
        wx.EVT_MENU(frame, cpframe.ID_EDIT_DELETE, self.on_remove_module)
        wx.EVT_MENU(frame, cpframe.ID_EDIT_DUPLICATE, self.on_duplicate_module)
        
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_TOGGLE,self.on_debug_toggle)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_STEP,self.on_debug_step)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_NEXT_IMAGE_SET,self.on_debug_next_image_set)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_NEXT_GROUP, self.on_debug_next_group)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_CHOOSE_GROUP, self.on_debug_choose_group)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_CHOOSE_IMAGE_SET, self.on_debug_choose_image_set)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_CHOOSE_RANDOM_IMAGE_SET, self.on_debug_random_image_set)
        wx.EVT_MENU(frame,cpframe.ID_DEBUG_RELOAD, self.on_debug_reload)

        # ~*~
        wx.EVT_MENU(frame, cpframe.ID_SAMPLE_INIT, self.on_sample_init)
        # ~^~
        
        wx.EVT_MENU(frame,cpframe.ID_WINDOW_SHOW_ALL_WINDOWS, self.on_show_all_windows)
        wx.EVT_MENU(frame,cpframe.ID_WINDOW_HIDE_ALL_WINDOWS, self.on_hide_all_windows)
        
        wx.EVT_MENU_OPEN(frame, self.on_frame_menu_open)
        
        cpp.evt_modulerunner_done(frame, self.on_module_runner_done)
        
    def start(self):
        '''Do initialization after GUI hookup
        
        Perform steps that need to happen after all of the user interface
        elements have been initialized.
        '''
        workspace_file = cpprefs.get_workspace_file()
        for attempt in range(1):
            if os.path.exists(workspace_file):
                try:
                    self.do_open_workspace(workspace_file, True)
                    break
                except:
                    pass
        else:
            self.do_create_workspace(workspace_file)
            self.__pipeline.clear()
        self.__workspace.add_notification_callback(
            self.on_workspace_event)
    
    def attach_to_pipeline_list_view(self,pipeline_list_view, movie_viewer):
        """Glom onto events from the list box with all of the module names in it
        
        """
        self.__pipeline_list_view = pipeline_list_view
        self.__movie_viewer = movie_viewer
        
    def attach_to_module_view(self,module_view):
        """Listen for setting changes from the module view
        
        """
        self.__module_view = module_view
        module_view.add_listener(self.__on_module_view_event)
    
    def attach_to_directory_view(self,directory_view):
        """Listen for requests to load pipelines
        
        """
        self.__directory_view = directory_view
        directory_view.add_pipeline_listener(self.__on_dir_load_pipeline)
    
    def attach_to_module_controls_panel(self,module_controls_panel):
        """Attach the pipeline controller to the module controls panel
        
        Attach the pipeline controller to the module controls panel.
        In addition, the PipelineController gets to add whatever buttons it wants to the
        panel.
        """
        self.__module_controls_panel = module_controls_panel
        mcp_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.__help_button = wx.Button(self.__module_controls_panel,-1,"?",(0,0), (30, -1))
        self.__help_button.SetToolTipString("Get Help for selected module")
        self.__mcp_text = wx.StaticText(self.__module_controls_panel,-1,"Adjust modules:")
        self.__mcp_add_module_button = wx.Button(self.__module_controls_panel,-1,"+",(0,0), (30, -1))
        self.__mcp_add_module_button.SetToolTipString("Add a module")
        self.__mcp_remove_module_button = wx.Button(self.__module_controls_panel,-1,"-",(0,0), (30, -1))
        self.__mcp_remove_module_button.SetToolTipString("Remove selected module")
        self.__mcp_module_up_button = wx.Button(self.__module_controls_panel,-1,"^",(0,0), (30, -1))
        self.__mcp_module_up_button.SetToolTipString("Move selected module up")
        self.__mcp_module_down_button = wx.Button(self.__module_controls_panel,-1,"v",(0,0), (30, -1))
        self.__mcp_module_down_button.SetToolTipString("Move selected module down")
        mcp_sizer.AddMany([(self.__help_button, 0, wx.ALIGN_CENTER | wx.ALL, 3),
                           ((1, 3), 3),
                           (self.__mcp_text, 0, wx.ALIGN_CENTER | wx.ALL, 3),
                           (self.__mcp_add_module_button, 0, wx.ALIGN_CENTER | wx.ALL, 3),
                           (self.__mcp_remove_module_button, 0, wx.ALIGN_CENTER | wx.ALL, 3),
                           (self.__mcp_module_up_button, 0, wx.ALIGN_CENTER | wx.ALL, 3),
                           (self.__mcp_module_down_button, 0, wx.ALIGN_CENTER | wx.ALL, 3)])
        self.__module_controls_panel.SetSizer(mcp_sizer)
        self.__module_controls_panel.Bind(wx.EVT_BUTTON, self.__on_help, self.__help_button)
        self.__module_controls_panel.Bind(wx.EVT_BUTTON, self.__on_add_module,self.__mcp_add_module_button)
        self.__module_controls_panel.Bind(wx.EVT_BUTTON, self.on_remove_module,self.__mcp_remove_module_button)
        self.__module_controls_panel.Bind(wx.EVT_BUTTON, self.on_module_up,self.__mcp_module_up_button)
        self.__module_controls_panel.Bind(wx.EVT_BUTTON, self.on_module_down,self.__mcp_module_down_button)

    def attach_to_test_controls_panel(self, test_controls_panel):
        """Attach the pipeline controller to the test controls panel
        
        Attach the pipeline controller to the test controls panel.
        In addition, the PipelineController gets to add whatever buttons it wants to the
        panel.
        """
        self.__test_controls_panel = test_controls_panel
        self.__tcp_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.__tcp_continue = wx.Button(test_controls_panel, -1, "Run", (0,0))
        self.__tcp_step = wx.Button(test_controls_panel, -1, "Step", (0,0))
        self.__tcp_next_imageset = wx.Button(test_controls_panel, -1, "Next Image", (0,0))
        self.__tcp_sizer.AddMany([(self.__tcp_continue, 0, wx.ALL | wx.EXPAND, 2),
                                  ((1, 1), 1),
                                  (self.__tcp_step, 0, wx.ALL | wx.EXPAND, 2),
                                  ((1, 1), 1),
                                  (self.__tcp_next_imageset, 0, wx.ALL | wx.EXPAND, 2)])
        self.__test_controls_panel.SetSizer(self.__tcp_sizer)
        self.__tcp_continue.SetToolTip(wx.ToolTip("Run to next pause"))
        self.__tcp_step.SetToolTip(wx.ToolTip("Step to next module"))
        self.__tcp_next_imageset.SetToolTip(wx.ToolTip("Jump to next image cycle"))
        self.__test_controls_panel.Bind(wx.EVT_BUTTON, self.on_debug_continue, self.__tcp_continue)
        self.__test_controls_panel.Bind(wx.EVT_BUTTON, self.on_debug_step, self.__tcp_step)
        self.__test_controls_panel.Bind(wx.EVT_BUTTON, self.on_debug_next_image_set, self.__tcp_next_imageset)

    def __on_open_workspace(self, event):
        '''Handle the Open Workspace menu command'''
        result = wx.MessageBox(
            "Do you want to use the current pipeline in your workspace?\n"
            '* Choose "Yes" to overwrite the workspace pipeline with\n'
            '  the pipeline that is currently open in CellProfiler.\n'
            '* Choose "No" to use the workspace\'s pipeline.\n'
            '* Choose "Cancel" if you do not want to open a workspace',
            'Overwrite current pipeline',
            wx.YES_NO | wx.CANCEL | wx.ICON_QUESTION, self.__frame)
        if result != wx.YES and result != wx.NO:
            return
        with wx.FileDialog(
            self.__frame,
            "Choose a workspace file to open",
            wildcard = "CellProfiler workspace (*.cpi)|*.cpi") as dlg:
            dlg.Directory = cpprefs.get_default_output_directory()
            if dlg.ShowModal() == wx.ID_OK:
                self.do_open_workspace(dlg.Path, result == wx.NO)
        
    def do_open_workspace(self, filename, load_pipeline):
        '''Open the given workspace file'''
        self.__workspace.load(filename, load_pipeline)
        cpprefs.set_workspace_file(filename)
        self.__pipeline.load_image_plane_details(self.__workspace)
        if not load_pipeline:
            self.__workspace.measurements.clear()
            self.__workspace.save_pipeline_to_measurements()
            
    def __on_new_workspace(self, event):
        '''Handle the New Workspace menu command'''
        with wx.FileDialog(
            self.__frame,
            "Choose the name for the new workspace file",
            wildcard = "CellProfiler workspace (*.cpi)|*.cpi",
            style = wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT) as dlg:
            dlg.Directory = cpprefs.get_default_output_directory()
            if dlg.ShowModal() == wx.ID_OK:
                self.do_create_workspace(dlg.Path)
        
    def do_create_workspace(self, filename):
        '''Create a new workspace file with the given name'''
        self.__workspace.create(filename)
        cpprefs.set_workspace_file(filename)
        self.__pipeline.clear_image_plane_details()
        self.__workspace.measurements.clear()
        self.__workspace.save_pipeline_to_measurements()
        
    def __on_save_as_workspace(self, event):
        '''Handle the Save Workspace As menu command'''
        with wx.FileDialog(
            self.__frame,
            "Save workspace file as",
            wildcard = "CellProfiler workspace (*.cpi)|*.cpi",
            style = wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT) as dlg:
            dlg.Directory = cpprefs.get_default_output_directory()
            if dlg.ShowModal() == wx.ID_OK:
                self.do_save_as_workspace(dlg.Path)
                
    def do_save_as_workspace(self, filename):
        '''Create a new copy of the workspace and change to it'''
        import h5py
        old_filename = self.__workspace.measurements.hdf5_dict.filename
        try:
            #
            # Note: shutil.copy and similar don't seem to work under Windows.
            #       I suspect that there's some file mapping magic that's
            #       causing problems because I found some internet postings
            #       where people tried to copy database files and failed.
            #       If you're thinking, "He didn't close the file", I did.
            #       shutil.copy creates a truncated file if you use it.
            #
            hdf5src = self.__workspace.measurements.hdf5_dict.hdf5_file
            hdf5dest = h5py.File(filename, mode="w")
            for key in hdf5src:
                obj = hdf5src[key]
                if isinstance(obj, h5py.Dataset):
                    hdf5dest[key] = obj.value
                else:
                    hdf5src.copy(hdf5src[key], hdf5dest, key)
            for key in hdf5src.attrs:
                hdf5dest.attrs[key] = hdf5src.attrs[key]
            hdf5dest.close()
            self.__workspace.load(filename, False)
            cpprefs.set_workspace_file(filename)
        except Exception, e:
            display_error_dialog(self.__frame, e, self.__pipeline,
                                 "Failed to save workspace",
                                 continue_only = True)
            self__workspace.load(old_filename, False)
        
    def __on_load_pipeline(self,event):
        if self.__dirty_pipeline:
            if wx.MessageBox('Do you want to save your current pipeline\n'
                             'before loading?', 'Save modified pipeline',
                             wx.YES_NO|wx.ICON_QUESTION, self.__frame) & wx.YES:
                self.do_save_pipeline()
        dlg = wx.FileDialog(self.__frame,
                            "Choose a pipeline file to open",
                            wildcard = ("CellProfiler pipeline (*.cp,*.mat)|*.cp;*.mat"))
        dlg.Directory = cpprefs.get_default_output_directory()
        if dlg.ShowModal()==wx.ID_OK:
            pathname = os.path.join(dlg.GetDirectory(),dlg.GetFilename())
            self.do_load_pipeline(pathname)
        dlg.Destroy()
            
    def __on_url_load_pipeline(self, event):
        if self.__dirty_pipeline:
            if wx.MessageBox('Do you want to save your current pipeline\n'
                             'before loading?', 'Save modified pipeline',
                             wx.YES_NO|wx.ICON_QUESTION, self.__frame) & wx.YES:
                self.do_save_pipeline()
        dlg = wx.TextEntryDialog(self.__frame,
                                 "Enter the pipeline's URL\n\n"
                                 "Example: https://svn.broadinstitute.org/"
                                 "CellProfiler/trunk/ExampleImages/"
                                 "ExampleSBSImages/ExampleSBS.cp",
                                 "Load pipeline via URL")
        if dlg.ShowModal() == wx.ID_OK:
            import urllib2
            self.do_load_pipeline(urllib2.urlopen(dlg.Value))
        dlg.Destroy()
    
    def __on_dir_load_pipeline(self,caller,event):
        if wx.MessageBox('Do you want to load the pipeline, "%s"?'%(os.path.split(event.Path)[1]),
                         'Load path', wx.YES_NO|wx.ICON_QUESTION ,self.__frame) & wx.YES:
            self.do_load_pipeline(event.Path)
    
    def do_load_pipeline(self,pathname):
        try:
            self.stop_debugging()
            if self.__running_pipeline:
                self.stop_running()
                self.__pipeline_measurements.close()
                self.__pipeline_measurements = None

            self.__pipeline.load(pathname)
            self.__pipeline.turn_off_batch_mode()
            self.__pipeline.load_image_plane_details(self.__workspace)
            self.__clear_errors()
            if isinstance(pathname, (str, unicode)):
                self.set_current_pipeline_path(pathname)
            self.__dirty_pipeline = False
            self.set_title()
            self.__workspace.save_pipeline_to_measurements()
            
        except Exception,instance:
            self.__frame.display_error('Failed during loading of %s'%(pathname),instance)

    def __clear_errors(self):
        for key,error in self.__setting_errors.iteritems():
            self.__frame.preferences_view.pop_error_text(error)
        self.__setting_errors = {}
        
    def __on_save_pipeline(self, event):
        path = cpprefs.get_current_pipeline_path()
        if path is None:
            self.do_save_pipeline()
        else:
            self.__pipeline.save(path)
            self.__dirty_pipeline = False
            self.set_title()
            self.__frame.preferences_view.set_message_text(
                "Saved pipeline to " + path)
            
    def __on_save_as_pipeline(self,event):
        try:
            self.do_save_pipeline()
        except Exception, e:
            wx.MessageBox('Exception:\n%s'%(e), 'Could not save pipeline...', wx.ICON_ERROR|wx.OK, self.__frame)
            
    def do_save_pipeline(self):
        '''Save the pipeline, asking the user for the name

        return True if the user saved the pipeline
        '''
        wildcard="CellProfiler pipeline (*.cp)|*.cp"
        dlg = wx.FileDialog(self.__frame,
                            "Save pipeline",
                            wildcard=wildcard,
                            style=wx.FD_SAVE|wx.FD_OVERWRITE_PROMPT)
        path = cpprefs.get_current_pipeline_path()
        if path is not None:
            dlg.Path = path
        else:
            dlg.Directory = cpprefs.get_default_output_directory()
        try:
            if dlg.ShowModal() == wx.ID_OK:
                file_name = dlg.GetFilename()
                if not sys.platform.startswith("win"):
                    if file_name.find('.') == -1:
                        # on platforms other than Windows, add the default suffix
                        file_name += ".cp"
                pathname = os.path.join(dlg.GetDirectory(), file_name)
                self.__pipeline.save(pathname)
                self.set_current_pipeline_path(pathname)
                self.__dirty_pipeline = False
                self.set_title()
                return True
            return False
        finally:
            dlg.Destroy()
    
    def __on_export_image_sets(self, event):
        '''Export the pipeline's image sets to a .csv file'''
        dlg = wx.FileDialog(self.__frame, "Export image sets",
                            wildcard = "Image set file (*.csv)|*.csv",
                            style = wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        try:
            if dlg.ShowModal() == wx.ID_OK:
                try:
                    self.__workspace.refresh_image_set()
                    self.__workspace.measurements.write_image_sets(dlg.Path)
                except Exception, e:
                    display_error_dialog(self.__frame, e, self.__pipeline,
                                         "Failed to export image sets",
                                         continue_only=True)
        finally:
            dlg.Destroy()
            
    def __on_plateviewer(self, event):
        import cellprofiler.gui.plateviewer as pv
        
        data = pv.PlateData()
        try:
            self.__workspace.refresh_image_set()
        except:
            display_error_dialog(self.__frame, e, self.__pipeline,
                                 "Failed to make image sets",
                                 continue_only=True)
            return
        m = self.__workspace.measurements
        assert isinstance(m, cpm.Measurements)
        
        url_features = [f for f in m.get_feature_names(cpm.IMAGE)
                        if f.startswith(cpm.C_URL)]
        image_numbers = m.get_image_numbers()
        pws = []
        for feature in ("Plate", "Well", "Site"):
            measurement = cpm.C_METADATA + "_" + feature
            if m.has_feature(cpm.IMAGE, measurement):
                pws.append(
                    m.get_measurement(cpm.IMAGE, measurement, image_numbers))
            else:
                pws.append([None] * len(image_numbers))
        plate, well, site = pws
        
        for url_feature in url_features:
            channel = [url_feature[(len(cpm.C_URL)+1):]] * len(image_numbers)
            data.add_files(
                m.get_measurement(cpm.IMAGE, url_feature, image_numbers),
                plate, well, site, channel_names = channel)
        if self.__plate_viewer is None:
            self.__pv_frame = wx.Frame(self.__frame, title = "Plate viewer")
        else:
            self.__pv_frame.DestroyChildren()
        self.__plate_viewer = pv.PlateViewer(self.__pv_frame, data)
        self.__pv_frame.Fit()
        self.__pv_frame.Show()
            
    
    def set_current_pipeline_path(self, pathname):
        cpprefs.set_current_pipeline_path(pathname)
        cpprefs.add_recent_file(pathname)
        self.populate_recent_files()
        
    def populate_recent_files(self):
        '''Populate the recent files menu'''
        recent_files = self.__frame.recent_files
        assert isinstance(recent_files, wx.Menu)
        while len(recent_files.MenuItems) > 0:
            self.__frame.Unbind(wx.EVT_MENU, id = recent_files.MenuItems[0].Id)
            recent_files.RemoveItem(recent_files.MenuItems[0])
        for index, file_name in enumerate(cpprefs.get_recent_files()):
            recent_files.Append(RECENT_FILE_MENU_ID[index], file_name)
            def on_recent_file(event, file_name = file_name):
                self.do_load_pipeline(file_name)
            self.__frame.Bind(wx.EVT_MENU,
                              on_recent_file,
                              id = RECENT_FILE_MENU_ID[index])
        
    def set_title(self):
        '''Set the title of the parent frame'''
        pathname = cpprefs.get_current_pipeline_path()
        if pathname is None:
            self.__frame.Title = "CellProfiler %s" % (version.title_string)
            return
        path, file = os.path.split(pathname)
        if self.__dirty_pipeline:
            self.__frame.Title = "CellProfiler %s: %s* (%s)" % (version.title_string, file, path)
        else:
            self.__frame.Title = "CellProfiler %s: %s (%s)" % (version.title_string, file, path)
            
    def __on_clear_pipeline(self,event):
        if wx.MessageBox("Do you really want to remove all modules from the pipeline?",
                         "Clearing pipeline",
                         wx.YES_NO | wx.ICON_QUESTION, self.__frame) == wx.YES:
            self.stop_debugging()
            if self.__running_pipeline:
                self.stop_running()            
                del self.__pipeline_measurements
                self.__pipeline_measurements = None
            self.__pipeline.clear()
            self.__clear_errors()
            cpprefs.set_current_pipeline_path(None)
            self.__dirty_pipeline = False
            self.set_title()
            self.enable_module_controls_panel_buttons()
    
    def check_close(self):
        '''Return True if we are allowed to close
        
        Check for pipeline dirty, return false if user doesn't want to close
        '''
        if self.__dirty_pipeline:
            #
            # Create a dialog box asking the user what to do.
            #
            dialog = wx.Dialog(self.__frame,
                               title = "Closing CellProfiler")
            super_sizer = wx.BoxSizer(wx.VERTICAL)
            dialog.SetSizer(super_sizer)
            #
            # This is the main window with the icon and question
            #
            sizer = wx.BoxSizer(wx.HORIZONTAL)
            super_sizer.Add(sizer, 1, wx.EXPAND|wx.ALL, 5)
            question_mark = wx.ArtProvider.GetBitmap(wx.ART_HELP,
                                                     wx.ART_MESSAGE_BOX)
            icon = wx.StaticBitmap(dialog, -1, question_mark)
            sizer.Add(icon, 0, wx.EXPAND | wx.ALL, 5)
            text = wx.StaticText(dialog, label = "Do you want to save the current pipeline?")
            sizer.Add(text, 0, wx.EXPAND | wx.ALL, 5)
            super_sizer.Add(wx.StaticLine(dialog), 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 20)
            #
            # These are the buttons
            #
            button_sizer = wx.BoxSizer(wx.HORIZONTAL)
            super_sizer.Add(button_sizer, 0, wx.ALIGN_CENTER_HORIZONTAL|wx.ALL, 5)
            SAVE_ID = wx.NewId()
            DONT_SAVE_ID = wx.NewId()
            RETURN_TO_CP_ID = wx.NewId()
            answer = [RETURN_TO_CP_ID]
            for button_id, text, set_default in (
                (SAVE_ID, "Save", True),
                (RETURN_TO_CP_ID, "Return to CellProfiler", False),
                (DONT_SAVE_ID, "Don't Save", False)):
                button = wx.Button(dialog, button_id, text)
                if set_default:
                    button.SetDefault()
                button_sizer.Add(button, 0, wx.EXPAND | wx.ALL, 5)
                def on_button(event, button_id = button_id):
                    dialog.SetReturnCode(button_id)
                    answer[0] = button_id
                    dialog.Close()
                dialog.Bind(wx.EVT_BUTTON, on_button, button,button_id)
            dialog.Fit()
            dialog.CentreOnParent()
            try:
                dialog.ShowModal()
                if answer[0] == SAVE_ID:
                    if not self.do_save_pipeline():
                        '''Cancel the closing if the user fails to save'''
                        return False
                elif answer[0] == RETURN_TO_CP_ID:
                    return False
            finally:
                dialog.Destroy()
        return True
    
    def on_close(self):
        self.close_debug_measurements()
        if self.__running_pipeline is not None:
            self.stop_running()
            del self.__pipeline_measurements
            self.__pipeline_measurements = None
    
    def __on_pipeline_event(self,caller,event):
        if isinstance(event,cpp.RunExceptionEvent):
            error_msg = None
            self.__pipeline_list_view.select_one_module(event.module.module_num)
            try:
                import MySQLdb
                if (isinstance(event.error, MySQLdb.OperationalError) and
                    len(event.error.args) > 1):
                    #
                    # The informative error is in args[1] for MySQL
                    #
                    error_msg = event.error.args[1]
            except:
                pass
            if error_msg is None:
                error_msg = str(event.error)
            message = (("Error while processing %s:\n"
                        "%s\n\nDo you want to stop processing?") %
                       (event.module.module_name,error_msg))
            result = display_error_dialog(self.__frame,
                                          event.error,
                                          self.__pipeline,
                                          message,
                                          event.tb)
            event.cancel_run = result == ED_STOP
            event.skip_thisset = result == ED_SKIP
                
        elif isinstance(event, cpp.LoadExceptionEvent):
            self.on_load_exception_event(event)
        elif event.is_pipeline_modification:
            self.__dirty_pipeline = True
            self.set_title()
            m = self.__workspace.measurements
            if event.is_image_set_modification:
                self.on_image_set_modification()
            self.__workspace.save_pipeline_to_measurements()
            
    def on_image_set_modification(self):
        self.__workspace.invalidate_image_set()
        self.exit_test_mode()
        
    def __on_image_directory_change(self, event):
        self.on_image_set_modification()
        
    def __on_output_directory_change(self, event):
        self.on_image_set_modification()
        
    def on_workspace_event(self, event):
        '''Workspace's file list changed. Invalidate the workspace cache.'''
        if isinstance(event, cpw.Workspace.WorkspaceFileListNotification):
            self.on_image_set_modification()
        
    def on_load_exception_event(self, event):
        '''Handle a pipeline load exception'''
        if event.module is None:
            module_name = event.module_name
        else:
            module_name = event.module.module_name
        if event.settings is None or len(event.settings) == 0:
            message = ("Error while loading %s: %s\nDo you want to stop processing?"%
                       (module_name, event.error.message))
        else:
            message = ("Error while loading %s: %s\n"
                       "Do you want to stop processing?\n\n"
                       "Module settings:\n"
                       "\t%s") % ( module_name,
                                   event.error.message,
                                   '\n\t'.join(event.settings))
        if wx.MessageBox(message, "Pipeline error",
                         wx.YES_NO | wx.ICON_ERROR, 
                         self.__frame) == wx.NO:
            event.cancel_run = False
        
    def enable_module_controls_panel_buttons(self):
        #
        # Enable/disable the movement buttons
        #
        selected_modules = self.__pipeline_list_view.get_selected_modules()
        enable_up = True
        enable_down = True
        enable_delete = True
        enable_duplicate = True
        if len(selected_modules) == 0:
            enable_up = enable_down = enable_delete = enable_duplicate = False
        else:
            if any([m.module_num == 1 for m in selected_modules]):
                enable_up = False
            if any([m.module_num == len(self.__pipeline.modules())
                    for m in selected_modules]):
                enable_down = False
        for menu_id, control, state in (
            (cpframe.ID_EDIT_MOVE_DOWN, self.__mcp_module_down_button, enable_down),
            (cpframe.ID_EDIT_MOVE_UP, self.__mcp_module_up_button, enable_up),
            (cpframe.ID_EDIT_DELETE, self.__mcp_remove_module_button, enable_delete),
            (cpframe.ID_EDIT_DUPLICATE, None, enable_duplicate)):
            if control is not None:
                control.Enable(state)
            menu_item = self.__frame.menu_edit.FindItemById(menu_id)
            if menu_item is not None:
                menu_item.Enable(state)
        
    def __on_help(self,event):
        modules = self.__get_selected_modules()
        if len(modules) > 0:
            self.__frame.do_help_modules(modules)
        else:
            wx.MessageBox(HELP_ON_MODULE_BUT_NONE_SELECTED, 
                          "No module selected",
                          style=wx.OK|wx.ICON_INFORMATION)
        
    def __on_add_module(self,event):
        if not self.__add_module_frame.IsShownOnScreen():
            x, y = self.__frame.GetPositionTuple()
            x = max(x - self.__add_module_frame.GetSize().width, 0)
            self.__add_module_frame.SetPosition((x, y))
        self.__add_module_frame.Show()
        self.__add_module_frame.Raise()
    
    def populate_edit_menu(self, menu):
        '''Display a menu of modules to add'''
        from cellprofiler.modules import get_module_names
        #
        # Get a two-level dictionary of categories and names
        #
        d = { "All": [] }
        for module_name in get_module_names():
            try:
                module = cellprofiler.modules.instantiate_module(module_name)
                category = module.category
                if isinstance(category, (str,unicode)):
                    categories = [category, "All"]
                else:
                    categories = list(category) + ["All"]
                for category in categories:
                    if not d.has_key(category):
                        d[category] = []
                    d[category].append(module_name)
            except:
                logger.error("Unable to instantiate module %s.\n\n" % 
                             module_name, exc_info=True)
         
        for category in sorted(d.keys()):
            sub_menu = wx.Menu()
            for module_name in sorted(d[category]):
                if self.module_name_to_menu_id.has_key(module_name):
                    menu_id = self.module_name_to_menu_id[module_name]
                else:
                    menu_id = wx.NewId()
                    self.module_name_to_menu_id[module_name] = menu_id
                    self.menu_id_to_module_name[menu_id] = module_name
                    self.__frame.Bind(wx.EVT_MENU, 
                                      self.on_menu_add_module, 
                                      id = menu_id)
                sub_menu.Append(menu_id, module_name)
            menu.AppendSubMenu(sub_menu, category)
            
    def on_menu_add_module(self, event):
        from cellprofiler.modules import instantiate_module
        assert isinstance(event, wx.CommandEvent)
        if self.menu_id_to_module_name.has_key(event.Id):
            module_name = self.menu_id_to_module_name[event.Id]
            module = instantiate_module(module_name)
            module.show_window = True  # default to show in GUI
            selected_modules = self.__get_selected_modules()
            if len(selected_modules) == 0:
                module.module_num = len(self.__pipeline.modules())+1
            else:
                module.module_num = selected_modules[0].module_num + 1
            self.__pipeline.add_module(module)
        else:
            logger.warn("Could not find module associated with ID = %d, module = %s" % (
                event.Id, event.GetString()))
            
        
    def __get_selected_modules(self):
        return self.__pipeline_list_view.get_selected_modules()
    
    def on_remove_module(self,event):
        self.remove_selected_modules()
    
    def remove_selected_modules(self):
        selected_modules = self.__get_selected_modules()
        for module in selected_modules:
            for setting in module.settings():
                if self.__setting_errors.has_key(setting.key()):
                    self.__frame.preferences_view.pop_error_text(self.__setting_errors.pop(setting.key()))                    
            self.__pipeline.remove_module(module.module_num)
        self.exit_test_mode()
        
    def exit_test_mode(self):
        '''Exit test mode with all the bells and whistles
        
        This is safe to call if not in test mode
        '''
        if self.is_in_debug_mode():
            self.stop_debugging()
            if cpprefs.get_show_exiting_test_mode_dlg():
                self.show_exiting_test_mode()

    def on_duplicate_module(self, event):
        self.duplicate_modules(self.__get_selected_modules())
        
    def duplicate_modules(self, modules):
        selected_modules = self.__get_selected_modules()
        if len(selected_modules):
            module_num=selected_modules[-1].module_num+1
        else:
            # insert module last if nothing selected
            module_num = len(self.__pipeline.modules())+1
        for m in modules:
            module = self.__pipeline.instantiate_module(m.module_name)
            module.module_num = module_num
            module.set_settings_from_values([str(s) for s in m.settings()], m.variable_revision_number, m.module_name, False)
            module.show_window = m.show_window  # copy visibility
            self.__pipeline.add_module(module)
            module_num += 1
            
            
    def on_module_up(self,event):
        """Move the currently selected modules up"""
        selected_modules = self.__get_selected_modules()
        for module in selected_modules:
            self.__pipeline.move_module(module.module_num,cpp.DIRECTION_UP);
        #
        # Major event - restart from scratch
        #
        if self.is_in_debug_mode():
            self.stop_debugging()
            if cpprefs.get_show_exiting_test_mode_dlg():
                self.show_exiting_test_mode()
        
    def on_module_down(self,event):
        """Move the currently selected modules down"""
        selected_modules = self.__get_selected_modules()
        selected_modules.reverse()
        for module in selected_modules:
            self.__pipeline.move_module(module.module_num,cpp.DIRECTION_DOWN);
        #
        # Major event - restart from scratch
        #
        if self.is_in_debug_mode():
            self.stop_debugging()
            if cpprefs.get_show_exiting_test_mode_dlg():
                self.show_exiting_test_mode()
            
    def on_undo(self, event):
        wx.BeginBusyCursor()
        try:
            if self.__pipeline.has_undo():
                self.__pipeline.undo()
        finally:
            wx.EndBusyCursor()
    
    def on_add_to_pipeline(self,caller,event):
        """Add a module to the pipeline using the event's module loader"""
        selected_modules = self.__get_selected_modules()
        if len(selected_modules):
            module_num=selected_modules[-1].module_num+1
        else:
            # insert module last if nothing selected
            module_num = len(self.__pipeline.modules())+1 
        module = event.module_loader(module_num)
        module.show_window = True  # default to show in GUI
        self.__pipeline.add_module(module)
        #
        # Major event - restart from scratch
        #
        #if self.is_in_debug_mode():
        #    self.stop_debugging()
        
    def __on_module_view_event(self,caller,event):
        assert isinstance(event,cellprofiler.gui.moduleview.SettingEditedEvent), '%s is not an instance of CellProfiler.CellProfilerGUI.ModuleView.SettingEditedEvent'%(str(event))
        setting = event.get_setting()
        proposed_value = event.get_proposed_value()
        setting.value = proposed_value
        module = event.get_module()
        is_image_set_modification = module.change_causes_prepare_run(setting)
        self.__pipeline.edit_module(event.get_module().module_num,
                                    is_image_set_modification)
        if self.is_in_debug_mode() and is_image_set_modification:
            #
            # If someone edits a really important setting in debug mode,
            # then you want to reset the debugger to reprocess the image set
            # list.
            #
            self.stop_debugging()
            if cpprefs.get_show_exiting_test_mode_dlg():
                self.show_exiting_test_mode()

    def status_callback(self, *args):
        self.__frame.preferences_view.on_pipeline_progress(*args)

    def on_run_multiple_pipelines(self, event):
        '''Menu handler for run multiple pipelines'''
        dlg = RunMultplePipelinesDialog(
                parent = self.__frame, 
                title = "Run multiple pipelines",
                style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER |wx.THICK_FRAME,
                size = (640,480))
        try:
            if dlg.ShowModal() == wx.ID_OK:
                self.pipeline_list = dlg.get_pipelines()
                self.run_next_pipeline(event)
        except:
            dlg.Destroy()
            
    def run_next_pipeline(self, event):
        if len(self.pipeline_list) == 0:
            return
        pipeline_details = self.pipeline_list.pop(0)
        self.do_load_pipeline(pipeline_details.path)
        cpprefs.set_default_image_directory(pipeline_details.default_input_folder)
        cpprefs.set_default_output_directory(pipeline_details.default_output_folder)
        cpprefs.set_output_file_name(pipeline_details.measurements_file)
        self.on_analyze_images(event)
        
    def on_analyze_images(self, event):
        '''Handle a user request to start running the pipeline'''
        ##################################
        #
        # Preconditions:
        # * Pipeline has no errors
        # * Default input and output directories are valid
        #
        ##################################
        
        ok, reason = self.__frame.preferences_view.check_preferences()
        if ok:
            try:
                self.__pipeline.test_valid()
            except cellprofiler.settings.ValidationError, v:
                ok = False
                reason = v.message
        if not ok:
            if wx.MessageBox("%s\nAre you sure you want to continue?" % reason,
                             "Problems with pipeline", wx.YES_NO) != wx.YES:
                self.pipeline_list = []
                return
        ##################################
        #
        # Start the pipeline
        #
        ##################################

        if cpanalysis.use_analysis:
            try:
                self.__module_view.disable()
                self.__frame.preferences_view.on_analyze_images()
                self.__workspace.refresh_image_set()
                self.__analysis = cpanalysis.Analysis(
                    self.__pipeline, 
                    self.get_output_file_path(),
                    initial_measurements=self.__workspace.measurements)
                self.__analysis.start(self.analysis_event_handler)

            except Exception, e:
                # Catastrophic failure
                display_error_dialog(self.__frame,
                                     e,
                                     self.__pipeline,
                                     "Failure in analysis startup.",
                                     sys.exc_info()[2],
                                     continue_only=True)
                self.stop_running()
            return

        output_path = self.get_output_file_path()
        if output_path:
            self.__module_view.disable()
            if self.__running_pipeline:
                self.__running_pipeline.close()
            self.__output_path = output_path
            self.__frame.preferences_view.on_analyze_images()
            if cpprefs.get_write_MAT_files() == cpprefs.WRITE_HDF5:
                if self.__pipeline_measurements is not None:
                    del self.__pipeline_measurements
                    self.__pipeline_measurements = None
                m = cpm.Measurements(filename = output_path)
            else:
                m = None
            self.__running_pipeline = self.__pipeline.run_with_yield(
                self.__frame,
                status_callback=self.status_callback,
                initial_measurements = m)
            try:
                # Start the first module.
                self.__pipeline_measurements = self.__running_pipeline.next()
            except StopIteration:
                #
                # Pipeline finished on the first go (typical for something
                # like CreateBatchFiles)
                #
                self.stop_running()
                if (self.__pipeline_measurements is not None and
                    cpprefs.get_write_MAT_files() is True):
                    self.__pipeline.save_measurements(self.__output_path, self.__pipeline_measurements)
                    self.__output_path = None
                    message = "Finished processing pipeline"
                    title = "Analysis complete"
                else:
                    message = "Pipeline processing finished, no measurements taken"
                    title = "Analysis complete"
                # allow cleanup of measurements
                del self.__pipeline_measurements
                self.__pipeline_measurements = None
                if len(self.pipeline_list) > 0:
                    self.run_next_pipeline(event)
                    return
                wx.MessageBox(message,title)
            except Exception, e:
                # Catastrophic failure on start
                display_error_dialog(self.__frame,
                                     e,
                                     self.__pipeline,
                                     "Failed to initialize pipeline",
                                     sys.exc_info()[2])
                if self.__pipeline_measurements is not None:
                    # try to leave measurements in a readable state
                    self.__pipeline_measurements.flush()
                self.stop_running()

    def analysis_event_handler(self, evt):
        PRI_EXCEPTION, PRI_INTERACTION, PRI_DISPLAY = range(3)

        if isinstance(evt, cpanalysis.AnalysisStarted):
            print "Analysis started"
        elif isinstance(evt, cpanalysis.AnalysisProgress):
            print "Progress", evt.counts
            total_jobs = sum(evt.counts.values())
            completed = evt.counts.get(cpanalysis.AnalysisRunner.STATUS_DONE, 0)
            wx.CallAfter(self.__frame.preferences_view.on_pipeline_progress, total_jobs, completed)
        elif isinstance(evt, cpanalysis.AnalysisFinished):
            print ("Cancelled!" if evt.cancelled else "Finished!")
            # drop any interaction/display requests or exceptions
            while True:
                try:
                    self.interaction_request_queue.get_nowait()  # in case the queue's been emptied
                except Queue.Empty:
                    break
            wx.CallAfter(self.stop_running)
        elif isinstance(evt, cpanalysis.DisplayRequest):
            wx.CallAfter(self.module_display_request, evt)
        elif isinstance(evt, cpanalysis.InteractionRequest):
            self.interaction_request_queue.put((PRI_INTERACTION, self.module_interaction_request, evt))
            wx.CallAfter(self.handle_analysis_feedback)
        elif isinstance(evt, cpanalysis.ExceptionReport):
            self.interaction_request_queue.put((PRI_EXCEPTION, self.analysis_exception, evt))
            wx.CallAfter(self.handle_analysis_feedback)
        elif isinstance(evt, cpanalysis.AnalysisPaused):
            print "Paused"
        elif isinstance(evt, cpanalysis.AnalysisResumed):
            print "Resumed"
        elif isinstance(evt, cellprofiler.pipeline.RunExceptionEvent):
            # exception in (prepare/post)_(run/group)
            import pdb
            pdb.post_mortem(evt.tb)
        else:
            raise ValueError("Unknown event type %s %s" % (type(evt), evt))

    def handle_analysis_feedback(self):
        '''Process any pending exception or interaction requests from the
        pipeline.  This function guards against multiple modal dialogs being
        opened, which can overwhelm the user and cause UI hangs.
        '''
        # just in case.
        assert wx.Thread_IsMain(), "PipelineController.handle_analysis_feedback() must be called from main thread!"

        # only one window at a time
        if self.interaction_pending:
            return

        try:
            pri_func_args = self.interaction_request_queue.get_nowait()  # in case the queue's been emptied
        except Queue.Empty:
            return

        self.interaction_pending = True
        try:
            pri_func_args[1](*pri_func_args[2:])
            if not self.interaction_request_queue.empty():
                wx.CallAfter(self.handle_analysis_feedback)
        finally:
            self.interaction_pending = False

    def module_display_request(self, evt):
        '''
        '''
        module_num = evt.module_num
        # use our shared workspace
        self.__workspace.display_data.__dict__.update(evt.display_data_dict)
        try:
            module = self.__pipeline.modules()[module_num - 1]
            if module.display != cpmodule.CPModule.display:
                fig = self.__workspace.get_module_figure(module,
                                                         evt.image_set_number,
                                                         self.__frame)
                fig.Raise()
                module.display(self.__workspace, fig)
                fig.Refresh()
        except:
            _, exc, tb = sys.exc_info()
            display_error_dialog(None, exc, self.__pipeline, tb=tb, continue_only=True,
                                 message="Exception in handling display request for module %s #%d" \
                                     % (module.module_name, module_num))
        finally:
            # we need to ensure that the reply_cb gets a reply
            evt.reply(cpanalysis.Ack())

    def module_interaction_request(self, evt):
        '''forward a module interaction request from the running pipeline to
        our own pipeline's instance of the module, and reply with the result.
        '''
        module_num = evt.module_num
        # extract args and kwargs from the request.
        # see main().interaction_handler() in analysis_worker.py
        args = [evt.__dict__['arg_%d' % idx] for idx in range(evt.num_args)]
        kwargs = dict((name, evt.__dict__['kwarg_%s' % name]) for name in evt.kwargs_names)
        result = ""
        try:
            module = self.__pipeline.modules()[module_num - 1]
            result = module.handle_interaction(*args, **kwargs)
        except:
            _, exc, tb = sys.exc_info()
            display_error_dialog(None, exc, self.__pipeline, tb=tb, continue_only=True,
                                 message="Exception in handling interaction request for module %s(#%d)" \
                                     % (module.module_name, module_num))
        finally:
            # we need to ensure that the reply_cb gets a reply (even if it
            # being empty causes futher exceptions).
            evt.reply(cpanalysis.InteractionReply(result=result))

    def analysis_exception(self, evt):
        '''Report an error in analysis to the user, giving options for
        skipping, aborting, and debugging.'''

        # The interaction here is getting a bit overly complex.  It would
        # probably be better to move all of this to a purely event-driven
        # model (dropping the Request/reply/reply/reply/... pattern).
        #
        # In that model, we get something like:
        # Exception -> display exception, reply with disposition (could be a
        #     debug request).  Do not close the window in this case.
        #
        # Debug requests generate a completely separate event:
        # DebugWaiting -> display information about where to connect to remote port.
        #
        # DebugDone -> using same display for the exception, reply with next disposition
        #
        # Should the error display go inactive until DebugDone?

        assert wx.Thread_IsMain(), "PipelineController.analysis_exception() must be called from main thread!"

        evtlist = [evt]

        def remote_debug(evtlist=evtlist):
            # choose a random string for verification
            verification = ''.join(random.choice(string.ascii_letters) for x in range(5))

            evt = evtlist[0]

            def port_callback(port):
                wx.MessageBox("Remote PDB waiting on port %d\nUse '%s' for verification" % (port, verification),
                              "Remote debugging started.",
                              wx.OK | wx.ICON_INFORMATION)
            # Request debugging.  We get back a port.
            port_reply = evt.reply(cpanalysis.ExceptionPleaseDebugReply(cpanalysis.DEBUG,
                                                                        hashlib.sha1(verification).hexdigest()),
                                   please_reply=True)
            port_callback(port_reply.port)
            # Acknowledge the port request, and we'll get back a
            # DebugComplete(), which we use as a new evt to reply with the
            # eventual CONTINUE/STOP choice.
            evtlist[0] = port_reply.reply(cpanalysis.Ack(), please_reply=True)

        if evt.module_name is not None:
            message = (("Error while processing %s:\n"
                        "%s\n\nDo you want to stop processing?") %
                       (evt.module_name, evt))
        else:
            message = (("Error while processing (remote worker):\n"
                        "%s\n\nDo you want to stop processing?") %
                       (evt))

        disposition = display_error_dialog(None, evt.exc_type, self.__pipeline, message,
                                           remote_exc_info=(evt.exc_type, evt.exc_message, evt.exc_traceback,
                                                            evt.filename, evt.line_number, remote_debug))
        if disposition == ED_STOP:
            self.__analysis.cancel()

        evtlist[0].reply(cpanalysis.Reply(disposition=disposition))

        wx.Yield()  # This allows cancel events to remove other exceptions from the queue.

    def on_restart(self, event):
        '''Restart a pipeline from a measurements file'''
        dlg = wx.FileDialog(self.__frame, "Select measurements file",
                            wildcard = "Measurements file (*.mat)|*.mat",
                            style = wx.FD_OPEN)
        try:
            if dlg.ShowModal() != wx.ID_OK:
                return
        finally:
            dlg.Destroy()
        
        ##################################
        #
        # Start the pipeline
        #
        ##################################
        output_path = self.get_output_file_path()
        if output_path:
            self.__module_view.disable()
            if self.__running_pipeline:
                self.__running_pipeline.close()
            self.__output_path = output_path
            self.__frame.preferences_view.on_analyze_images()
            self.__running_pipeline = \
                self.__pipeline.restart_with_yield(dlg.Path, self.__frame,
                                                   self.status_callback)
            try:
                # Start the first module.
                self.__pipeline_measurements = self.__running_pipeline.next()
            except StopIteration:
                #
                # Pipeline finished on the first go (typical for something
                # like CreateBatchFiles)
                #
                self.stop_running()
                if (self.__pipeline_measurements is not None and 
                    cpprefs.get_write_MAT_files() is True):
                    self.__pipeline.save_measurements(self.__output_path,self.__pipeline_measurements)
                    del self.__pipeline_measurements
                    self.__pipeline_measurements = None
                    self.__output_path = None
                    message = "Finished processing pipeline"
                    title = "Analysis complete"
                else:
                    message = "Pipeline processing finished, no measurements taken"
                    title = "Analysis complete"
                if len(self.pipeline_list) > 0:
                    self.run_next_pipeline(event)
                    return
                wx.MessageBox(message,title)
                
    def on_pause(self, event):
        if not self.__pause_pipeline:
            self.__frame.preferences_view.pause(True)
            self.__pause_pipeline = True
            if cpanalysis.use_analysis:
                self.__analysis.pause()
            # This is necessary for the case where the user hits pause
            # then resume during the time a module is executing, which
            # results in two calls to __running_pipeline.next() trying
            # to execute simultaneously if the resume causes a
            # ModuleRunnerDoneEvent.
            self.__need_unpause_event = False 
        else:
            self.__frame.preferences_view.pause(False)
            self.__pause_pipeline = False
            if cpanalysis.use_analysis:
                self.__analysis.resume()
            if self.__need_unpause_event:
                # see note above
                cpp.post_module_runner_done_event(self.__frame)
        
    def on_frame_menu_open(self, event):
        pass
    
    def on_stop_running(self,event):
        self.pipeline_list = []
        if (self.__analysis is not None) and self.__analysis.check_running():
            self.__analysis.cancel()
            return  # self.stop_running() will be called when we receive the
                    # AnalysisCancelled event in self.analysis_event_handler.
        self.stop_running()
        if self.__pipeline_measurements is not None:
            self.save_measurements()
        del self.__pipeline_measurements
        self.__pipeline_measurements = None
    
    def on_save_measurements(self, event):
        if self.__pipeline_measurements is not None:
            self.save_measurements()
        
    def save_measurements(self):
        if cpprefs.get_write_MAT_files() == cpprefs.WRITE_HDF5:
            return
        dlg = wx.FileDialog(self.__frame,
                            "Save measurements to a file",
                            wildcard="CellProfiler measurements (*.mat)|*.mat",
                            style = wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        try:
            if dlg.ShowModal() == wx.ID_OK:
                pathname = os.path.join(dlg.GetDirectory(), dlg.GetFilename())
                self.__pipeline.save_measurements(pathname, 
                                                  self.__pipeline_measurements)
        finally:
            dlg.Destroy()
        
    def stop_running(self):
        self.__analysis = None
        if self.__running_pipeline is not None:
            self.__running_pipeline.close()
            self.__running_pipeline = None
        self.__pause_pipeline = False
        self.__frame.preferences_view.on_stop_analysis()
        self.__module_view.enable()
    
    def is_in_debug_mode(self):
        """True if there's some sort of debugging in progress"""
        return self.__debug_image_set_list != None
    
    def on_debug_toggle(self, event):
        if self.is_in_debug_mode():
            self.on_debug_stop(event)
        else:
            self.on_debug_start(event)
            
    def on_debug_start(self, event):
        self.__pipeline_list_view.select_one_module(1)
        self.__movie_viewer.Value = 0
        self.start_debugging()
    
    def start_debugging(self):
        self.__pipeline_list_view.set_debug_mode(True)
        self.__frame.preferences_view.start_debugging()
        self.__test_controls_panel.Show()
        self.__test_controls_panel.GetParent().GetSizer().Layout()
        self.__pipeline.test_mode = True
        try:
            if not self.__workspace.refresh_image_set():
                raise ValueError("Failed to get image sets")
            if self.__workspace.measurements.image_set_count == 0:
                wx.MessageBox("The pipeline did not identify any image sets. "
                              "Please check your settings for any modules that "
                              "load images and try again.",
                              "No image sets.",
                              wx.OK | wx.ICON_ERROR, self.__frame)
                self.stop_debugging()
                return False
        except ValueError, v:
            message = "Error while preparing for run:\n%s"%(v)
            wx.MessageBox(message, "Pipeline error", wx.OK | wx.ICON_ERROR, self.__frame)
            self.stop_debugging()
            return False
        
        self.close_debug_measurements()
        self.__debug_measurements = cellprofiler.measurements.Measurements(
            copy = self.__workspace.measurements,
            mode="memory")
        self.__debug_object_set = cpo.ObjectSet(can_overwrite=True)
        self.__frame.enable_debug_commands()
        assert isinstance(self.__pipeline, cpp.Pipeline)
        self.__debug_image_set_list = cpi.ImageSetList(True)
        workspace = cpw.Workspace(self.__pipeline, None, None, None,
                                  self.__debug_measurements,
                                  self.__debug_image_set_list,
                                  self.__frame)
        workspace.set_file_list(self.__workspace.file_list)
        self.__keys, self.__groupings = self.__pipeline.get_groupings(
            workspace)

        self.__grouping_index = 0
        self.__within_group_index = 0
        self.__pipeline.prepare_group(workspace,
                                      self.__groupings[0][0],
                                      self.__groupings[0][1])
        self.__debug_outlines = {}
        return True
    
    def close_debug_measurements(self):
        del self.__debug_measurements
        self.__debug_measurements = None
        
    def on_debug_stop(self, event):
        self.stop_debugging()

    def stop_debugging(self):
        self.__pipeline_list_view.set_debug_mode(False)
        self.__frame.preferences_view.stop_debugging()
        self.__test_controls_panel.Hide()
        self.__test_controls_panel.GetParent().GetSizer().Layout()
        self.__frame.enable_debug_commands(False)
        self.__debug_image_set_list = None
        self.close_debug_measurements()
        self.__debug_object_set = None
        self.__debug_outlines = None
        self.__debug_grids = None
        self.__pipeline_list_view.on_stop_debugging()
        self.__pipeline.test_mode = False
        self.__pipeline.end_run()
    
    def on_debug_step(self, event):
        
        modules = self.__pipeline_list_view.get_selected_modules()
        module = modules[0]
        self.do_step(module)
    
    def do_step(self, module):
        """Do a debugging step by running a module
        """
        failure = 1
        old_cursor = self.__frame.GetCursor()
        self.__frame.SetCursor(wx.StockCursor(wx.CURSOR_WAIT))
        try:
            image_set_number = self.__debug_measurements.image_set_number
            self.__debug_measurements.add_image_measurement(
                cpp.GROUP_NUMBER, self.__grouping_index)
            self.__debug_measurements.add_image_measurement(
                cpp.GROUP_INDEX, self.__within_group_index)
            workspace = cpw.Workspace(self.__pipeline,
                                      module,
                                      self.__debug_measurements,
                                      self.__debug_object_set,
                                      self.__debug_measurements,
                                      self.__debug_image_set_list,
                                      self.__frame if module.show_window else None,
                                      outlines = self.__debug_outlines)
            self.__debug_grids = workspace.set_grids(self.__debug_grids)
            module.run(workspace)
            if module.show_window:
                fig = workspace.get_module_figure(module, image_set_number)
                module.display(workspace, fig)
                fig.Refresh()
            workspace.refresh()
            if workspace.disposition == cpw.DISPOSITION_SKIP:
                last_module_num = self.__pipeline.modules()[-1].module_num
                self.__pipeline_list_view.select_one_module(last_module_num)
                self.last_debug_module()
            elif module.module_num < len(self.__pipeline.modules()):
                self.__pipeline_list_view.select_one_module(module.module_num+1)
            failure=0
        except Exception,instance:
            logger.error("Failed to run module %s", module.module_name,
                         exc_info=True)
            event = cpp.RunExceptionEvent(instance,module)
            self.__pipeline.notify_listeners(event)
            if event.cancel_run:
                self.on_debug_stop(event)
                failure=-1
            failure=1
        self.__frame.SetCursor(old_cursor)
        if ((module.module_name != 'Restart' or failure==-1) and
            self.__debug_measurements != None):
            module_error_measurement = 'ModuleError_%02d%s'%(module.module_num,module.module_name)
            self.__debug_measurements.add_measurement('Image',
                                                      module_error_measurement,
                                                      failure);
        return failure==0
    
    def current_debug_module(self):
        assert self.is_in_debug_mode()
        module_idx = self.__movie_viewer.Value
        return self.__pipeline.modules()[module_idx]

    def next_debug_module(self):
        if self.__movie_viewer.Value < len(self.__pipeline.modules()) - 1:
            self.__movie_viewer.Value += 1
            self.__movie_viewer.Refresh()
            return True
        else:
            return False
        
    def last_debug_module(self):
        self.__movie_viewer.Value = len(self.__pipeline.modules()) - 1

    def on_debug_step(self, event):
        if len(self.__pipeline.modules()) == 0:
            return
        success = self.do_step(self.current_debug_module())
        if success:
            self.next_debug_module()
        
    def on_debug_continue(self, event):
        if len(self.__pipeline.modules()) == 0:
            return
        while True:
            module = self.current_debug_module()
            success = self.do_step(module)
            if not success:
                return
            if not self.next_debug_module():
                return
            if self.current_debug_module().wants_pause:
                return

    def on_debug_next_image_set(self, event):
        #
        # We have two indices, one into the groups and one into
        # the image indexes within the groups
        #
        keys, image_numbers = self.__groupings[self.__grouping_index]
        if len(image_numbers) == 0:
            return
        self.__within_group_index = ((self.__within_group_index + 1) % 
                                     len(image_numbers))
        image_number = image_numbers[self.__within_group_index]
        self.__debug_measurements.next_image_set(image_number)
        self.__pipeline_list_view.select_one_module(1)
        self.__movie_viewer.Value = 0
        self.__debug_outlines = {}

    def on_debug_prev_image_set(self, event):
        keys, image_numbers = self.__groupings[self.__grouping_index]
        self.__within_group_index = ((self.__within_group_index + len(image_numbers) - 1) % 
                                     len(image_numbers))
        image_number = image_numbers[self.__within_group_index]
        self.__debug_measurements.next_image_set(image_number)
        self.__pipeline_list_view.select_one_module(1)
        self.__movie_viewer.Value = 0
        self.__debug_outlines = {}

    def on_debug_next_group(self, event):
        if self.__grouping_index is not None:
            self.debug_choose_group(((self.__grouping_index + 1) % 
                               len(self.__groupings)))
    
    def on_debug_prev_group(self, event):
        if self.__grouping_index is not None:
            self.debug_choose_group(((self.__grouping_index + len(self.__groupings) - 1) % 
                               len(self.__groupings)))
            
    def on_debug_random_image_set(self,event):
        group_index = 0 if len(self.__groupings) == 1 else numpy.random.randint(0,len(self.__groupings)-1,size=1)
        keys, image_numbers = self.__groupings[group_index]
        if len(image_numbers) == 0:
            return
        numpy.random.seed()
        image_number_index = numpy.random.randint(1,len(image_numbers),size=1)[0]
        self.__within_group_index = ((image_number_index-1) % len(image_numbers))
        image_number = image_numbers[self.__within_group_index]
        self.__debug_measurements.next_image_set(image_number)
        self.__pipeline_list_view.select_one_module(1)
        self.__movie_viewer.Value = 0
        self.__debug_outlines = {}
        
    def debug_choose_group(self, index):
        self.__grouping_index = index
        self.__within_group_index = 0
        workspace = cpw.Workspace(self.__pipeline, None, None, None,
                                  self.__debug_measurements,
                                  self.__debug_image_set_list,
                                  self.__frame)
        
        self.__pipeline.prepare_group(workspace,
                                      self.__groupings[self.__grouping_index][0],
                                      self.__groupings[self.__grouping_index][1])
        key, image_numbers = self.__groupings[self.__grouping_index]
        image_number = image_numbers[self.__within_group_index]
        self.__debug_measurements.next_image_set(image_number)
        self.__pipeline_list_view.select_one_module(1)
        self.__movie_viewer.Value = 0
        self.__debug_outlines = {}
            
    def on_debug_choose_group(self, event):
        '''Choose a group'''
        if len(self.__groupings) < 2:
            wx.MessageBox("There is only one group and it is currently running in test mode","Choose image group")
            return
        dialog = wx.Dialog(self.__frame, title="Choose an image group", style=wx.RESIZE_BORDER|wx.DEFAULT_DIALOG_STYLE)
        super_sizer = wx.BoxSizer(wx.VERTICAL)
        dialog.SetSizer(super_sizer)
        super_sizer.Add(wx.StaticText(dialog, label = "Select a group set for testing:"),0,wx.EXPAND|wx.ALL,5)
        choices = []
        
        for grouping, image_numbers in self.__groupings:
            text = ["%s=%s"%(k,v) for k,v in grouping.iteritems()]
            text = ', '.join(text)
            choices.append(text)
        lb = wx.ListBox(dialog, -1, choices=choices)
        lb.Select(0)
        super_sizer.Add(lb, 1, wx.EXPAND|wx.ALL, 10)
        super_sizer.Add(wx.StaticLine(dialog),0,wx.EXPAND|wx.ALL,5)
        btnsizer = wx.StdDialogButtonSizer()
        btnsizer.AddButton(wx.Button(dialog, wx.ID_OK))
        btnsizer.AddButton(wx.Button(dialog, wx.ID_CANCEL))
        btnsizer.Realize()
        super_sizer.Add(btnsizer)
        super_sizer.Add((2,2))
        dialog.Fit()
        dialog.CenterOnParent()
        try:
            if dialog.ShowModal() == wx.ID_OK:
                self.debug_choose_group(lb.Selection)
        finally:
            dialog.Destroy()
    
    def on_debug_choose_image_set(self, event):
        '''Choose one of the current image sets
        
        '''
        dialog = wx.Dialog(self.__frame, title="Choose an image cycle", style=wx.RESIZE_BORDER|wx.DEFAULT_DIALOG_STYLE)
        super_sizer = wx.BoxSizer(wx.VERTICAL)
        dialog.SetSizer(super_sizer)
        super_sizer.Add(wx.StaticText(dialog, label = "Select an image cycle for testing:"),0,wx.EXPAND|wx.ALL,5)
        choices = []
        indexes = []
        m = self.__debug_measurements
        features = [f for f in 
                    m.get_feature_names(cpm.IMAGE)
                    if f.split("_")[0] in (cpm.C_METADATA, C_FILE_NAME,
                                           C_PATH_NAME, C_FRAME)]
        for image_number in self.__groupings[self.__grouping_index][1]:
            indexes.append(image_number)
            text = ', '.join([
                "%s=%s" % (f, m.get_measurement(cpm.IMAGE, f, 
                                                image_set_number = image_number))
                for f in features])
                                                              
            choices.append(text)
        if len(choices) == 0:
            wx.MessageBox("Sorry, there are no available images. Check your LoadImages module's settings",
                          "Can't choose image")
            return
        lb = wx.ListBox(dialog, -1, choices=choices)
        lb.Select(0)
        super_sizer.Add(lb, 1, wx.EXPAND|wx.ALL, 10)
        super_sizer.Add(wx.StaticLine(dialog),0,wx.EXPAND|wx.ALL,5)
        btnsizer = wx.StdDialogButtonSizer()
        btnsizer.AddButton(wx.Button(dialog, wx.ID_OK))
        btnsizer.AddButton(wx.Button(dialog, wx.ID_CANCEL))
        btnsizer.Realize()
        super_sizer.Add(btnsizer)
        super_sizer.Add((2,2))
        dialog.Fit()
        dialog.CenterOnParent()
        try:
            if dialog.ShowModal() == wx.ID_OK:
                image_number = indexes[lb.Selection]
                self.__debug_measurements.next_image_set(image_number)
                self.__pipeline_list_view.select_one_module(1)
                self.__movie_viewer.Value = 0
                for i, (grouping, image_numbers) in enumerate(self.__groupings):
                    if image_number in image_numbers:
                        self.__grouping_index = i
                        self.__within_group_index = \
                            list(image_numbers).index(image_number)
                        break
        finally:
            dialog.Destroy()

    def on_debug_reload(self, event):
        '''Reload modules from source, warning the user if the pipeline could
        not be reinstantiated with the new versions.

        '''
        success = self.__pipeline.reload_modules()
        if not success:
            wx.MessageBox(("CellProfiler has reloaded modules from source, but "
                           "couldn't reinstantiate the pipeline with the new modules.\n"
                           "See the log for details."),
                          "Error reloading modules.",
                          wx.ICON_ERROR | wx.OK)

    def on_sample_init(self, event):
        if self.__module_view != None:
            if self.__module_view.get_current_module() != None:
                self.show_parameter_sample_options(
                    self.__module_view.get_current_module().get_module_num(), event)
            else:
                print "No current module"

    def show_parameter_sample_options(self, module_num, event):
        if self.__parameter_sample_frame == None:
            selected_module = self.__pipeline.module(module_num)
            selected_module.test_valid(self.__pipeline)

            top_level_frame = self.__frame
            self.parameter_sample_frame = psf.ParameterSampleFrame(
                top_level_frame, selected_module, self.__pipeline, -1)
            self.parameter_sample_frame.Bind(
                wx.EVT_CLOSE, self.on_parameter_sample_frame_close)
            self.parameter_sample_frame.Show(True)

    def on_parameter_sample_frame_close(self, event):
        event.Skip()
        self.__parameter_sample_frame = None

    # ~^~
    def on_module_runner_done(self,event):
        '''Run one iteration of the pipeline
        
        Called in response to a
        cpp.ModuleRunnerDoneEvent whenever a module
        is done running.
        '''
        if self.__pause_pipeline:
            # see note in self.on_pause()
            self.__need_unpause_event = True
        elif self.__running_pipeline:
            try:
                wx.Yield()
                # if the user hits "Stop", self.__running_pipeline can go away
                if self.__running_pipeline:
                    self.__pipeline_measurements = self.__running_pipeline.next()
                    event.RequestMore()
            except StopIteration:
                self.stop_running()
                if (self.__pipeline_measurements != None and 
                    cpprefs.get_write_MAT_files() is True):
                    self.__frame.preferences_view.set_message_text(
                        WRITING_MAT_FILE)
                    try:
                        self.__pipeline.save_measurements(self.__output_path,
                                                          self.__pipeline_measurements)
                        self.__frame.preferences_view.set_message_text(WROTE_MAT_FILE)
                    except IOError, err:
                        while True:
                            result = wx.MessageBox(
                                ("CellProfiler could not save your measurements. "
                                 "Do you want to try saving it using a different name?\n"
                                 "The error was:\n%s") % (err), 
                                "Error saving measurements.", 
                                wx.ICON_ERROR|wx.YES_NO)
                            if result == wx.YES:
                                try:
                                    self.save_measurements()
                                    self.__frame.preferences_view.set_message_text(WROTE_MAT_FILE)
                                    break
                                except IOError, err:
                                    self.__frame.preferences_view.set_message_text("")
                                    pass
                            else:
                                self.__frame.preferences_view.set_message_text("")
                                break
                    self.__output_path = None
                self.__running_pipeline = None
                del self.__pipeline_measurements
                self.__pipeline_measurements = None
                if len(self.pipeline_list) > 0:
                    self.run_next_pipeline(event)
                    return
                #
                # A little dialog with a "save pipeline" button in addition
                # to the "OK" button.
                #
                if cpprefs.get_show_analysis_complete_dlg():
                    self.show_analysis_complete()
                    
    def show_analysis_complete(self):
        '''Show the "Analysis complete" dialog'''
        dlg = wx.Dialog(self.__frame, -1, "Analysis complete")
        sizer = wx.BoxSizer(wx.VERTICAL)
        dlg.SetSizer(sizer)
        sub_sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(sub_sizer, 1, wx.EXPAND)
        font = wx.SystemSettings.GetFont(wx.SYS_SYSTEM_FONT)
        text_ctrl = wx.StaticText(dlg, 
                                  label="Finished processing pipeline.")
        text_ctrl.Font = font
        sub_sizer.Add(
            text_ctrl,
            1, wx.ALIGN_CENTER_HORIZONTAL | wx.ALIGN_CENTER_VERTICAL | 
            wx.EXPAND | wx.ALL, 10)
        bitmap = wx.ArtProvider.GetBitmap(wx.ART_INFORMATION,
                                          wx.ART_CMN_DIALOG,
                                          size=(32,32))
        sub_sizer.Add(wx.StaticBitmap(dlg, -1, bitmap), 0,
                      wx.EXPAND | wx.ALL, 10)
        dont_show_again = wx.CheckBox(dlg, -1, "Don't show this again")
        dont_show_again.Value = False
        sizer.Add(dont_show_again, 0, 
                  wx.ALIGN_LEFT | wx.ALIGN_CENTER_VERTICAL | wx.LEFT, 10)
        button_sizer = wx.StdDialogButtonSizer()
        save_pipeline_button = wx.Button(dlg, -1, "Save pipeline")
        button_sizer.AddButton(save_pipeline_button)
        button_sizer.SetCancelButton(save_pipeline_button)
        button_sizer.AddButton(wx.Button(dlg, wx.ID_OK))
        sizer.Add(button_sizer, 0, 
                  wx.ALIGN_CENTER_HORIZONTAL | wx.EXPAND | wx.ALL, 10)
        dlg.Bind(wx.EVT_BUTTON, self.__on_save_pipeline, 
                 save_pipeline_button)
        button_sizer.Realize()
        dlg.Fit()
        dlg.CenterOnParent()
        try:
            dlg.ShowModal()
            if dont_show_again.Value:
                cpprefs.set_show_analysis_complete_dlg(False)
        finally:
            dlg.Destroy()
            
    def show_exiting_test_mode(self):
        '''Show the "Analysis complete" dialog'''
        dlg = wx.Dialog(self.__frame, -1, "Exiting test mode")
        sizer = wx.BoxSizer(wx.VERTICAL)
        dlg.SetSizer(sizer)
        sub_sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(sub_sizer, 1, wx.EXPAND)
        text_ctrl = wx.StaticText(dlg, 
                                  label=("You have changed the pipeline so\n"
                                         "that test mode will now exit.\n"))
        sub_sizer.Add(
            text_ctrl,
            1, wx.ALIGN_CENTER_HORIZONTAL | wx.ALIGN_CENTER_VERTICAL | 
            wx.EXPAND | wx.ALL, 10)
        bitmap = wx.ArtProvider.GetBitmap(wx.ART_INFORMATION,
                                          wx.ART_CMN_DIALOG,
                                          size=(32,32))
        sub_sizer.Add(wx.StaticBitmap(dlg, -1, bitmap), 0,
                      wx.EXPAND | wx.ALL, 10)
        dont_show_again = wx.CheckBox(dlg, -1, "Don't show this again")
        dont_show_again.Value = False
        sizer.Add(dont_show_again, 0, 
                  wx.ALIGN_LEFT | wx.ALIGN_CENTER_VERTICAL | wx.LEFT, 10)
        button_sizer = wx.StdDialogButtonSizer()
        button_sizer.AddButton(wx.Button(dlg, wx.ID_OK))
        sizer.Add(button_sizer, 0, 
                  wx.ALIGN_CENTER_HORIZONTAL | wx.EXPAND | wx.ALL, 10)
        button_sizer.Realize()
        dlg.Fit()
        dlg.CenterOnParent()
        try:
            dlg.ShowModal()
            if dont_show_again.Value:
                cpprefs.set_show_exiting_test_mode_dlg(False)
        finally:
            dlg.Destroy()
            
    def get_output_file_path(self):
        path = os.path.join(cpprefs.get_default_output_directory(),
                            cpprefs.get_output_file_name())
        if os.path.exists(path) and not cpprefs.get_allow_output_file_overwrite():
            (first_part,ext)=os.path.splitext(path)
            start = 1
            match = re.match('^(.+)__([0-9]+)$',first_part)
            if match:
                first_part = match.groups()[0]
                start = int(match.groups()[1])
            for i in range(start,1000):
                alternate_name = '%(first_part)s__%(i)d%(ext)s'%(locals())
                if not os.path.exists(alternate_name):
                    break
            result = wx.MessageDialog(parent=self.__frame,
                                message='%s already exists. Would you like to create %s instead?'%(path, alternate_name),
                                caption='Output file exists',
                                style = wx.YES_NO+wx.ICON_QUESTION)
            user_choice = result.ShowModal()
            result.Destroy()
            if user_choice & wx.YES:
                path = alternate_name
                cpprefs.set_output_file_name(os.path.split(alternate_name)[1])
            else:
                return None
        return path
    
    def on_show_all_windows(self, event):
        '''Turn "show_window" on for every module in the pipeline'''
        for module in self.__pipeline.modules():
            module.show_window = True
        self.__dirty_pipeline = True
        self.set_title()
        
    def on_hide_all_windows(self, event):
        '''Turn "show_window" off for every module in the pipeline'''
        for module in self.__pipeline.modules():
            module.show_window = False
        self.__dirty_pipeline = True
        self.set_title()
            
    def run_pipeline(self):
        """Run the current pipeline, returning the measurements
        """
        return self.__pipeline.Run(self.__frame)
    
