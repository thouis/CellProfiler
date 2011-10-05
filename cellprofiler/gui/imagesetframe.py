'''imagesetdlg.py - baby example of how to create imagesets

burn down and redo please
'''

#
# GRRRR GRRRR - Wing is insisting that the python path
# starts in this directory.
#
import logging
logger = logging.getLogger(__name__)
import re
import os
import sys
if __name__=="__main__":
    p = os.path.split(os.path.split(os.path.split(__file__)[0])[0])[0]
    sys.path.insert(0, p)
import wx
import wx.aui

import cellprofiler.project as cpproject
from cellprofiler.sqlite_project import SQLiteProject

KWD_ALL_URLS = "All urls"
KWD_ALL_IMAGES = "All images / one channel"

class ImageSetFrame(wx.Frame):
    def __init__(self, *args, **kwargs):
        if kwargs.has_key("path"):
            path = kwargs["path"]
            kwargs = kwargs.copy()
            del kwargs["path"]
        else:
            path = None
        super(self.__class__, self).__init__(*args, **kwargs)
        
        self.pane_name_and_menu_item = []
        self._mgr = wx.aui.AuiManager()
        self._mgr.SetManagedWindow(self)
        self._mgr.AddPane(self.create_urlset_pane(), 
                          wx.aui.AuiPaneInfo()
                          .Left()
                          .BestSize((240, 480))
                          .Name("urlset")
                          .Caption("URL set"))
        self._mgr.AddPane(self.create_imageset_pane(), 
                          wx.aui.AuiPaneInfo()
                          .Center()
                          .Name("imageset")
                          .Caption("Image set"))
        
        self.create_imageset_pane = CreateImagesetPane(self)
        self._mgr.AddPane(self.create_imageset_pane,
                          wx.aui.AuiPaneInfo()
                          .Float()
                          .FloatingSize((640,480))
                          .Name("createimageset")
                          .Caption("Create image set")
                          .Hide())
        #
        # Toolbars
        #
        toolbar = wx.ToolBar(self, style = wx.TB_TEXT)
        label = wx.StaticText(toolbar, -1, "Image set:")
        toolbar.AddControl(label)
        self.imageset_ctrl = wx.Choice(toolbar)
        self.imageset_ctrl.SetTransparent(0)
        toolbar.AddControl(self.imageset_ctrl)
        tool = toolbar.AddLabelTool(
            -1, "Delete image set",
            wx.ArtProvider.GetBitmap(wx.ART_DELETE, wx.ART_TOOLBAR))
        toolbar.Bind(wx.EVT_TOOL, self.on_delete_imageset, tool)
        toolbar.Realize()
        self._mgr.AddPane(toolbar, wx.aui.AuiPaneInfo()
                          .ToolbarPane()
                          .Top()
                          .Row(1)
                          .Name("imagesetchooser")
                          .Caption("Image set chooser"))
        
        def on_imageset_change(event):
            self.current_imageset = self.imageset_ctrl.GetStringSelection()
            self.update_imageset_pane()
        self.Bind(wx.EVT_CHOICE, on_imageset_change, self.imageset_ctrl)
        
                          
        mb = wx.MenuBar()
        self.SetMenuBar(mb)
        file_menu = wx.Menu()
        mb.Append(file_menu, "File")
        open_item = file_menu.Append(-1, "Open")
        assert isinstance(open_item, wx.MenuItem)
        self.Bind(wx.EVT_MENU, self.on_open, id = open_item.Id)
        save_item = file_menu.Append(-1, "Save")
        def on_save(event):
            self.project.commit()
        self.Bind(wx.EVT_MENU, on_save, id=save_item.Id)
        dir_menu = wx.Menu()
        mb.Append(dir_menu, "Directory")
        add_item = dir_menu.Append(-1, "Add")
        self.Bind(wx.EVT_MENU, self.on_directory_add, id = add_item.Id)
        
        view_menu = wx.Menu()
        mb.Append(view_menu, "View")
        
        for text, help, pane_name, check in (
            ("URL set", "Show or hide the URL set view", "urlset", True),
            ("Image set", "Show or hide the Image set view", "imageset", True),
            ("Image set chooser", "Show or hide the image set chooser toolbar", 
             "imagesetchooser", True),
            ("Create image", "Show or hide the image set creation pane",
             "createimageset", False)):
            item = view_menu.Append(-1, text, help, wx.ITEM_CHECK)
            item.Check(check)
            self.pane_name_and_menu_item.append((pane_name, item))
            def on_view_toggle(event, pane_name = pane_name):
                self.on_view_toggle(event, pane_name)
            self.Bind(wx.EVT_MENU_RANGE, on_view_toggle, id = item.Id)
        
        self.Bind(wx.aui.EVT_AUI_PANE_CLOSE, self.on_pane_close)
        if path is None:
            self.SetPath(":memory:")
        else:
            self.SetPath(path)
        self._mgr.Update()
        self.Show()
        
    def on_view_toggle(self, event, pane_name):
        if event.Checked():
            self._mgr.GetPane(pane_name).Show()
        else:
            self._mgr.GetPane(pane_name).Hide()
        self._mgr.Update()
        
    def on_pane_close(self, event):
        '''Hide panes, don't close them'''
        pane = event.GetPane()
        pane.Hide()
        self._mgr.Update()
        event.Veto()
        items = [x[1] for x in self.pane_name_and_menu_item
                 if x[0] == pane.name]
        for item in items:
            item.Check(False)
            
    def create_urlset_pane(self):
        pane = self.urlset_pane = wx.SplitterWindow(self)
        self.urlset_tree = wx.TreeCtrl(pane)
        self.urlset_tree.AddRoot("All images")
        self.urlset_table = wx.ListCtrl(pane, style = wx.LC_REPORT)
        pane.SplitHorizontally(self.urlset_tree, self.urlset_table)
        return pane
        
    def update_urlset_pane(self):
        self.urlset_tree.DeleteChildren(self.urlset_tree.RootItem)
        urlsets = self.project.get_urlset_names()
        for urlset in urlsets:
            self.urlset_tree.AddChild(self.urlset_tree.RootItem, urlset)
        self.urlset_table.DeleteAllItems()
        self.urlset_table.DeleteAllColumns()
        keys = self.project.get_metadata_keys()
        self.urlset_table.InsertColumn(0, "URL")
        for i, key in enumerate(keys):
            self.urlset_table.InsertColumn(i+1, key)
        for row in self.project.get_images_by_metadata(keys):
            x = [self.project.get_url(row[-1])]
            x += row[:-1]
            item = self.urlset_table.Append(x)
        self.urlset_pane.Layout()
        
    def create_imageset_pane(self):
        self.imageset_table = wx.ListCtrl(self, style = wx.LC_REPORT)
        return self.imageset_table
    
    def on_delete_imageset(self, event):
        imageset_name = self.imageset_ctrl.GetStringSelection()
        if len(imageset_name) > 0:
            self.project.remove_imageset(imageset_name)
        self.update_imageset_choices()
        self.update_imageset_pane()
            
    def update_imageset_choices(self):
        choices = list(self.project.get_imageset_names())
        old_choices = self.imageset_ctrl.GetStrings()
        if (len(choices) != len(old_choices) 
            or not all([x in old_choices for x in choices])):
            old_selection = self.imageset_ctrl.GetStringSelection()
            self.imageset_ctrl.SetItems(choices)
            if old_selection in choices:
                self.imageset_ctrl.SetSelection(choices.find(old_selection))
            else:
                self.update_imageset_pane()
        
    def update_imageset_pane(self):
        self.imageset_table.DeleteAllItems()
        self.imageset_table.DeleteAllColumns()
        if self.current_imageset is not None:
            row_count = self.project.get_imageset_row_count(self.current_imageset)
            channels = self.project.get_imageset_channels(self.current_imageset)
            self.imageset_table.InsertColumn(0, "Image Number")
            for i, channel in enumerate(channels):
                self.imageset_table.InsertColumn(i+1, channel)
            for i in range(1, row_count+1):
                result = self.project.get_imageset_row_images(
                    self.current_imageset, i)
                row = [str(i)]
                for j, channel in enumerate(channels):
                    if result.has_key(channel):
                        image_ids = result[channel]
                        if len(image_ids) > 1:
                            row.append("Duplicate")
                        else:
                            row.append(self.project.get_url(image_ids[0]))
                    else:
                        row.append("Missing")
                self.imageset_table.Append(row)
                         
    def on_open(self, event):
        dlg = wx.FileDialog(self, "Open project", 
                            wildcard = "Project files (*.cpproj)|*.cpproj",
                            style = wx.FD_OPEN)
        try:
            if dlg.ShowModal() == wx.ID_OK:
                self.SetPath(dlg.Path)
        finally:
            dlg.Destroy()
        
    def SetPath(self, path):
        self.path = path
        self.project = cpproject.open_project(path, SQLiteProject)
        self.current_imageset = None
        self.update_urlset_pane()
        self.update_imageset_choices()
        self.create_imageset_pane.set_project(self.project)
        
    def GetPath(self,path):
        return self.path
    
    def on_directory_add(self, event):
        dlg = AddDirectoryDlg(self)
        try:
            if dlg.ShowModal() == wx.ID_OK:
                self.project.add_directory(dlg.path)
                for filename in os.listdir(dlg.path):
                    pathname = os.path.join(dlg.path, filename)
                    result = re.search(dlg.regexp, pathname)
                    if result is not None:
                        d = result.groupdict()
                        image_id = self.project.add_url(pathname)
                        self.project.add_image_metadata(d.keys(), d.values(),
                                                        image_id)
            self.update_urlset_pane()
            self.create_imageset_pane.update()
        finally:
            dlg.Destroy()
            
class CreateImagesetPane(wx.Panel):
    def __init__(self, parent, *args, **kwargs):
        super(self.__class__, self).__init__(parent, *args, **kwargs)
        self.imageset_frame = parent
        self.project = None
        
        self.SetSizer(wx.BoxSizer(wx.VERTICAL))
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        self.Sizer.Add(sizer, 0, wx.EXPAND | wx.ALL, 5)
        self.imageset_name_label = wx.StaticText(self, -1, "Image set name:")
        sizer.Add(self.imageset_name_label, 0, wx.LEFT | wx.ALIGN_CENTER_VERTICAL)
        self.imageset_name_ctrl = wx.TextCtrl(self)
        sizer.Add(self.imageset_name_ctrl, 1, wx.EXPAND)
        
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.Sizer.Add(sizer, 0, wx.EXPAND | wx.ALL, 5)
        sizer.Add(wx.StaticText(self, -1, "URL set:"), 0, 
                  wx.ALIGN_LEFT| wx.ALIGN_CENTER_VERTICAL)
        
        self.urlset_choice = wx.Choice(self)
        sizer.Add(self.urlset_choice, 1, wx.EXPAND)
        self.Bind(wx.EVT_CHOICE, self.on_urlset_change, self.urlset_choice)

        sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.Sizer.Add(sizer, 0, wx.EXPAND | wx.ALL, 5)
        label = wx.StaticText(self, -1, "Channel key:")
        sizer.Add(label, 0, wx.ALIGN_LEFT | wx.ALIGN_CENTER_VERTICAL)
        self.channel_choice = wx.Choice(self)
        sizer.Add(self.channel_choice, 1, wx.EXPAND)
        self.Bind(wx.EVT_CHOICE, self.on_channel_change, self.channel_choice)
        
        self.channel_name_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.Sizer.Add(self.channel_name_sizer, 0, wx.EXPAND | wx.ALL, 5)
        self.channel_label = wx.StaticText(self, -1, "Channel name:")
        self.channel_name_sizer.Add(self.channel_label,
                                    0, wx.ALIGN_LEFT | wx.ALIGN_CENTER_VERTICAL)
        self.channel_name_ctrl = wx.TextCtrl(self)
        self.channel_name_sizer.Add(self.channel_name_ctrl, 1, wx.EXPAND)
        #
        #
        self.channel_map_ctrl = wx.ListCtrl(
            self, style = wx.LC_REPORT | wx.LC_EDIT_LABELS)
        
        imagelist = wx.ImageList(16, 16)
        self.checkbox_bitmaps = {}
        for state_flag in (0, wx.CONTROL_CHECKED):
            for selection_flag in (0, wx.CONTROL_CURRENT):
                flag = state_flag | selection_flag
                checkbox_bitmap = get_checkbox_bitmap(
                    self.channel_map_ctrl, flag, 16, 16)
                idx = imagelist.Add(checkbox_bitmap)
                self.checkbox_bitmaps[flag] = (idx, checkbox_bitmap)
        
        self.channel_map_ctrl.SetImageList(imagelist, wx.IMAGE_LIST_SMALL)
        self.imagelist = imagelist
        info = wx.ListItem()
        info.m_mask = wx.LIST_MASK_TEXT | wx.LIST_MASK_IMAGE | wx.LIST_MASK_FORMAT
        info.m_image = -1
        info.m_text = "Metadata value"
        info.m_format = 0
        self.channel_map_ctrl.InsertColumnInfo(0, info)
        info.m_text = "Channel name"
        self.channel_map_ctrl.InsertColumnInfo(1, info)
        self.channel_map_ctrl.Bind(wx.EVT_LEFT_DOWN, self.on_channel_ctl_click)
        
        self.Sizer.Add(self.channel_map_ctrl, 0, wx.EXPAND | wx.ALL, 5)
        
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.Sizer.Add(sizer, 1, wx.EXPAND | wx.ALL, 5)
        self.from_listbox = wx.ListBox(self, style = wx.LB_MULTIPLE)
        sizer.Add(self.from_listbox, 1, wx.EXPAND)
        
        button_sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(button_sizer, 0, wx.EXPAND)
        
        self.button_map = {}
        for art, callback in (
            ( wx.ART_GO_UP, self.on_up),
            ( wx.ART_GO_FORWARD, self.on_add),
            ( wx.ART_GO_BACK, self.on_remove),
            ( wx.ART_GO_DOWN, self.on_down)):
            button = self.button_map[art] = wx.BitmapButton(
                self, -1, wx.ArtProvider.GetBitmap(art, wx.ART_BUTTON, (16,16)))
            button.Enable(False)
            button_sizer.Add(button, 0, wx.ALIGN_CENTER_VERTICAL )
            self.Bind(wx.EVT_BUTTON, callback, button)
        self.to_listbox = wx.ListBox(self, style = wx.LB_MULTIPLE)
        sizer.Add(self.to_listbox, 1, wx.EXPAND)
        
        button = self.create_imageset_button = wx.Button(self, -1, "Create image set")
        self.Sizer.Add(button, 0, wx.EXPAND | wx.ALL, 5)
        self.Bind(wx.EVT_BUTTON, self.on_create_imageset, button)
        self.Bind(wx.EVT_LISTBOX, self.on_select_from, self.from_listbox)
        self.Bind(wx.EVT_LISTBOX, self.on_select_to, self.to_listbox)
        self.channel_name_ctrl.Bind(wx.EVT_TEXT, self.on_channel_name_change)
        self.imageset_name_ctrl.Bind(wx.EVT_TEXT, self.on_imageset_name_change)
        self.Layout()
        self.Show()
        
    def set_project(self, project):
        assert isinstance(project, cpproject.Project)
        self.project = project
        self.update()
        
    def update(self):
        keys = self.project.get_metadata_keys()
        self.urlset_choice.Clear()
        self.urlset_choice.Append(KWD_ALL_URLS)
        for urlset in self.project.get_urlset_names():
            self.urlset_choice.Append(urlset)
        self.urlset_choice.SetSelection(0)
        self.channel_choice.Clear()
        self.channel_choice.Append(KWD_ALL_IMAGES)
        self.from_listbox.Clear()
        for key in keys:
            self.from_listbox.Append(key)
            self.channel_choice.Append(key)
        self.channel_choice.SetSelection(0)
        for button in self.button_map.values():
            button.Enable(False)
        self.validate()
        
    def on_up(self, event):
        d = {}
        last_unselected_key = None
        for i, key in enumerate(self.to_listbox.GetStrings()):
            if not self.to_listbox.IsSelected(i):
                d[key] = (i, False)
                last_unselected_key = key
            else:
                d[key] = (i-1, True)
                d[last_unselected_key] = (i, False)
        inv = dict([(i, (key, selected)) for key, (i, selected) in d.items()])
        self.to_listbox.Clear()
        for i in range(len(d)):
            key, selected = inv[i]
            self.to_listbox.Append(key)
            if selected:
                self.to_listbox.SetSelection(i)
        self.enable_disable_up_down()
    
    def on_down(self, event):
        d = {}
        last_unselected_key = None
        for i, key in reversed(list(enumerate(self.to_listbox.GetStrings()))):
            if not self.to_listbox.IsSelected(i):
                d[key] = (i, False)
                last_unselected_key = key
            else:
                d[key] = (i+1, True)
                d[last_unselected_key] = (i, False)
        inv = dict([(i, (key, selected)) for key, (i, selected) in d.items()])
        self.to_listbox.Clear()
        for i in range(len(d)):
            key, selected = inv[i]
            self.to_listbox.Append(key)
            if selected:
                self.to_listbox.SetSelection(i)
        self.enable_disable_up_down()
    
    def on_remove(self, event):
        to_remove = []
        for i, key in enumerate(self.to_listbox.GetStrings()):
            if self.to_listbox.IsSelected(i):
                self.from_listbox.Append(key)
                to_remove.insert(0, i)
        for i in to_remove:
            self.to_listbox.Delete(i)
        self.button_map[wx.ART_GO_BACK].Enable(False)
        self.button_map[wx.ART_GO_UP].Enable(False)
        self.button_map[wx.ART_GO_DOWN].Enable(False)
        self.validate()
    
    def on_add(self, event):
        to_remove = []
        for i, key in enumerate(self.from_listbox.GetStrings()):
            if self.from_listbox.IsSelected(i):
                self.to_listbox.Append(key)
                to_remove.insert(0, i)
        for i in to_remove:
            self.from_listbox.Delete(i)
        self.button_map[wx.ART_GO_FORWARD].Enable(False)
        self.validate()
    
    def on_select_from(self, event):
        selected = [x for i,x in enumerate(self.from_listbox.GetStrings())
                    if self.from_listbox.IsSelected(i)]
        other = self.to_listbox.GetStrings()
        self.button_map[wx.ART_GO_FORWARD].Enable(len(selected))
    
    def on_select_to(self, event):
        selected = [x for i,x in enumerate(self.to_listbox.GetStrings())
                    if self.to_listbox.IsSelected(i)]
        other = self.from_listbox.GetStrings()
        self.button_map[wx.ART_GO_BACK].Enable(len(selected))
        self.enable_disable_up_down()
        self.validate()
        
    def on_urlset_change(self, event):
        self.validate()
    
    def on_channel_change(self, event):
        self.channel_name_sizer.Show(not self.channel_selected)
        if self.channel_selected:
            self.channel_map_ctrl.DeleteAllItems()
            channel_values = self.project.get_metadata_values(
                self.channel_choice.GetStringSelection())
            for channel in channel_values:
                index = self.channel_map_ctrl.InsertImageStringItem(
                    sys.maxint, channel,
                    self.checkbox_bitmaps[wx.CONTROL_CHECKED][0])
                self.channel_map_ctrl.SetStringItem(index, 1, channel)
        self.validate()
        self.Sizer.Layout()
    
    def get_channel_ctl_item_check(self, index):
        return (self.channel_map_ctrl.GetItem(index).m_image == 
                self.checkbox_bitmaps[wx.CONTROL_CHECKED][0])
    
    def on_channel_ctl_click(self, event):
        item, where = self.channel_map_ctrl.HitTest(event.GetPosition())
        if where & wx.LIST_HITTEST_ONITEMICON > 0:
            if self.get_channel_ctl_item_check(item):
                image = self.checkbox_bitmaps[0][0]
            else:
                image = self.checkbox_bitmaps[wx.CONTROL_CHECKED][0]
            self.channel_map_ctrl.SetItemImage(item, image)
        
    def on_channel_name_change(self, event):
        self.validate()
        
    def on_imageset_name_change(self, event):
        self.validate()

    @property
    def channel_selected(self):
        '''True if some key is selected as the channel metadata'''
        return not self.channel_choice.GetStringSelection() == KWD_ALL_IMAGES
    
    @property
    def urlset_selected(self):
        '''True if some urlset is selected. False if action applies to all URLS'''
        return not self.urlset_choice.GetStringSelection() == KWD_ALL_URLS
    
    @property 
    def keys_selected(self):
        '''True if some metadata keys are selected in the to_listbox'''
        return any([self.to_listbox.IsSelected(i)
                            for i in range(self.to_listbox.GetCount())])
    
    @property
    def has_keys(self):
        return self.to_listbox.GetCount() > 0
    
    def validate(self):
        '''Set the text of the "create imageset" button based on selections'''
        enable_button = True
        if len(self.imageset_name_ctrl.Value) == 0:
            enable_button = False
            self.imageset_name_label.SetForegroundColour(wx.RED)
        else:
            self.imageset_name_label.SetForegroundColour(wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOWTEXT))
        self.imageset_name_label.Refresh()

        if self.has_keys:
            title = "Create image set by metadata"
        else:
            title = "Create image set by order (not yet supported)"
            enable_button = False
        if self.urlset_selected:
            title += " using images in %s" % self.urlset_choice.GetSelection()
        else:
            title += " using all images"
        if self.channel_selected:
            title += " using %s to assign urls to channels" % self.channel_choice.GetSelection()
            self.channel_map_ctrl.Show()
            self.channel_name_sizer.ShowItems(False)
        else:
            channel_name = self.channel_name_ctrl.Value
            if len(channel_name) == 0:
                self.channel_label.SetForegroundColour(wx.RED)
                title += " assigning all urls to a single channel"
                enable_button = False
            else:
                self.channel_label.SetForegroundColour(
                    wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOWTEXT))
                title += (" assigning all urls to the channel, %s" 
                          % self.channel_name_ctrl.Value)
            self.channel_label.Refresh()
            self.channel_name_sizer.ShowItems(True)
            self.channel_map_ctrl.Hide()
        self.create_imageset_button.SetToolTipString(title)
        self.create_imageset_button.Enable(enable_button)
        
    def enable_disable_up_down(self):
        item_count = self.to_listbox.GetCount()
        self.button_map[wx.ART_GO_UP].Enable(
            self.keys_selected and not self.to_listbox.IsSelected(0))
        self.button_map[wx.ART_GO_DOWN].Enable(
            self.keys_selected 
            and not self.to_listbox.IsSelected(item_count - 1))
    
    def on_create_imageset(self, event):
        keys = self.to_listbox.GetStrings()
        if self.channel_selected:
            channel = self.channel_choice.GetStringSelection()
        else:
            channel = None
        if self.urlset_selected:
            urlset = self.urlset_choice.GetStringSelection()
        else:
            urlset = None
        self.project.create_imageset(self.imageset_name_ctrl.Value,
                                     keys, channel, urlset = urlset)
        self.imageset_frame.update_imageset_choices()
    
class AddDirectoryDlg(wx.Dialog):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.SetSizer(wx.BoxSizer(wx.VERTICAL))
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.Sizer.Add(sizer, 1, wx.EXPAND | wx.ALL, 5)
        sizer.Add(wx.StaticText(self, -1, "Directory:"), 0, wx.LEFT)
        path_ctrl = wx.TextCtrl(self)
        sizer.Add(path_ctrl, 1, wx.EXPAND)
        self.path = ""
        def on_path_change(event):
            self.path = path_ctrl.Value
        path_ctrl.Bind(wx.EVT_TEXT, on_path_change)
        browse_button = wx.Button(self, -1, "Browse")
        sizer.Add(browse_button, 0, wx.EXPAND)
        def on_browse(event):
            dirdlg=wx.DirDialog(browse_button, "Choose directory")
            if dirdlg.ShowModal() == wx.ID_OK:
                path_ctrl.Value = dirdlg.Path
                self.path = dirdlg.Path
        browse_button.Bind(wx.EVT_BUTTON, on_browse)
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.Sizer.Add(sizer, 1, wx.EXPAND| wx.ALL, 5)
        sizer.Add(wx.StaticText(self, -1, "Regexp:"), 0, wx.LEFT)
        regexp_ctrl = wx.TextCtrl(self)
        self.regexp = ""
        def on_regexp_change(event):
            self.regexp = regexp_ctrl.Value
        regexp_ctrl.Bind(wx.EVT_TEXT, on_regexp_change)
        sizer.Add(regexp_ctrl, 1, wx.EXPAND)
        buttons_sizer = wx.StdDialogButtonSizer()
        buttons_sizer.AddButton(wx.Button(self, wx.ID_OK))
        buttons_sizer.AddButton(wx.Button(self, wx.ID_CANCEL))
        buttons_sizer.Realize()
        self.Sizer.Add(buttons_sizer, 0 , wx.ALIGN_CENTER_HORIZONTAL)
        self.Fit()
    
def get_checkbox_bitmap(window, flags, width, height):
    '''Return a bitmap with a checkbox drawn into it
    
    flags - rendering flags including CONTROL_CHECKED and CONTROL_UNDETERMINED
    width, height - size of bitmap to return
    '''
    dc = wx.MemoryDC()
    bitmap = wx.EmptyBitmap(width, height)
    dc.SelectObject(bitmap)
    dc.SetBrush(wx.BLACK_BRUSH)
    dc.SetTextForeground(wx.BLACK)
    try:
        dc.Clear()
        render = wx.RendererNative.Get()
        render.DrawCheckBox(window, dc, (0, 0, width, height), flags)
    finally:
        dc.SelectObject(wx.NullBitmap)
    dc.Destroy()
    return bitmap
    
if __name__=="__main__":
    app = wx.PySimpleApp(False)
    
    frame = ImageSetFrame(None)
    app.MainLoop()
    
