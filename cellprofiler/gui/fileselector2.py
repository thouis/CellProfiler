import wx
import wx.html
import sys
import os
import re
import time
import urllib

import PIL

try:
    from collections import OrderedDict
except:
    # http://pypi.python.org/pypi/ordereddict
    from ordereddict import OrderedDict

from bioformats.formatreader import fetch_metadata

import PIL.Image
SUPPORTED_IMAGE_EXTENSIONS = set(PIL.Image.EXTENSION.keys())
SUPPORTED_IMAGE_EXTENSIONS.add(".mat")  # NOOoooo!... ok, fine.
# movies
SUPPORTED_IMAGE_EXTENSIONS.update(['.avi', '.mpeg', '.stk', '.flex', '.mov', '.tif',
                                  '.tiff', '.zvi'])
# bioformats
SUPPORTED_IMAGE_EXTENSIONS.update([
        ".1sc", ".2fl", ".afm", ".aim", ".avi", ".co1", ".flex", ".fli", ".gel",
        ".ics", ".ids", ".im", ".img", ".j2k", ".lif", ".lsm", ".mpeg", ".pic",
        ".pict", ".ps", ".raw", ".svs", ".stk", ".tga", ".zvi"])


def extract_metadata(filename):
    mdlist = fetch_metadata(filename)
    for file_metadata in mdlist:
        md = OrderedDict()
        md['Filename'] = os.path.basename(filename)
        md['Pathname'] = os.path.dirname(filename)
        # XXX - should check for existing parameters
        md['URL'] = 'file://' + filename.replace(os.path.sep, '/') + '#' + \
            urllib.urlencode([('series', file_metadata.get('series', '')), \
                                  ('index', file_metadata.get('index', ''))])
        file_metadata.pop('URL', '')
        md.update(file_metadata)
        yield md

def substring_filter(val):
    return lambda s: (s is not None) and (val in s)

def regexp_filter(val):
    pat = re.compile(val)
    return lambda s: (s is not None) and pat.search(s)

def equals_filter(val):
    return lambda s: s == val

def equal_or_none_filter(val):
    return lambda s: (s is None) or (s == val)

def has_metadata_filter(val):
    # ignore val
    return lambda s: s is not None

class Clause(wx.Panel):
    FILTER_CHOICES = OrderedDict([('contains', dict(filter=substring_filter)),
                                  ('does not contain', dict(filter=substring_filter, inverted=True)),
                                  ('matches regexp', dict(filter=regexp_filter)),
                                  ("doesn't match regexp", dict(filter=regexp_filter, inverted=True)),
                                  ('equals', dict(filter=equals_filter)),
                                  ('does not equal', dict(filter=equals_filter, inverted=True)),
                                  ('is missing or equals', dict(filter=equal_or_none_filter)),
                                  ('is present', dict(filter=has_metadata_filter, takes_value=False)),
                                  ('is missing', dict(filter=has_metadata_filter, takes_value=False, inverted=True))])

    def __init__(self, parent):
        wx.Panel.__init__(self, parent)

        self.sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.metadata_col = wx.Choice(self, choices=['Filename', 'Pathname'])  # defaults
        self.filter_choice = wx.Choice(self, choices=self.FILTER_CHOICES.keys())
        self.value = wx.TextCtrl(self, value='', style=wx.TE_PROCESS_ENTER)
        add_button = wx.Button(self, label='+', style=wx.BU_EXACTFIT)
        remove_button = self.remove_button = wx.Button(self, label='-', style=wx.BU_EXACTFIT)

        self.sizer.Add(self.metadata_col)
        self.sizer.AddSpacer(2)
        self.sizer.Add(self.filter_choice)
        self.sizer.AddSpacer(2)
        self.sizer.Add(self.value, proportion=1)
        self.sizer.AddSpacer(2)
        self.sizer.Add(add_button)
        self.sizer.AddSpacer(1)
        self.sizer.Add(remove_button)

        # add a border to show highlighting of active clause
        border_sizer = wx.BoxSizer(wx.HORIZONTAL)
        border_sizer.Add(self.sizer, proportion=1, flag=wx.ALL | wx.EXPAND, border=2)
        self.SetSizer(border_sizer)

        self.metadata_col.Bind(wx.EVT_CHOICE, self.update)
        self.filter_choice.Bind(wx.EVT_CHOICE, self.update)
        self.value.Bind(wx.EVT_TEXT_ENTER, self.update)
        self.value.Bind(wx.EVT_SET_FOCUS, self.set_focus)
        self.value.Bind(wx.EVT_KILL_FOCUS, self.lose_focus)
        add_button.Bind(wx.EVT_BUTTON, self.add)
        remove_button.Bind(wx.EVT_BUTTON, self.remove)

    def update(self, evt):
        filter_choice = self.filter_choice.GetStringSelection()
        self.sizer.Show(self.value, show=self.FILTER_CHOICES[filter_choice].get('takes_value', True))
        self.set_focus(evt)
        self.Layout()
        self.Refresh()

    def set_focus(self, evt):
        self.BackgroundColour = '#ff7777'
        self.Parent.set_focus(self)
        # update the metadata choices after asking the parent to update.  This
        # will call self's own filter() method, storing the list of available
        # columns.
        previous_metadata_col = self.metadata_col.StringSelection
        if not previous_metadata_col in self.available_metadata_cols:
            # XXX - Probably should warn user.  Especially if the missing data
            # was previously a column generated by a regexp clause
            self.available_metadata_cols[previous_metadata_col] = ''
        self.metadata_col.Items = self.available_metadata_cols.keys()
        self.metadata_col.StringSelection = previous_metadata_col

    def lose_focus(self, evt):
        self.BackgroundColour = wx.SystemSettings.GetColour(wx.SYS_COLOUR_BACKGROUND)

    def add(self, evt):
        self.Parent.add_after_clause(self)

    def remove(self, evt):
        self.Parent.remove_clause(self)

    def show_remove_button(self, show):
        self.sizer.Show(self.remove_button, show)
        self.Layout()

    def filter(self, filelist):
        try:
            self.available_metadata_cols = OrderedDict()
            filter_info = self.FILTER_CHOICES[self.filter_choice.StringSelection]
            filt = filter_info['filter'](self.value.Value)
            inverted = filter_info.get('inverted', False)
            idx = self.metadata_col.StringSelection
            for pf in filelist:
                self.available_metadata_cols.update(pf)  # to update metadata columns choices in self.set_focus()
                if ((inverted and not filt(unicode(pf.get(idx, None)))) or
                    (filt(unicode(pf.get(idx, None))) and not inverted)):
                    yield pf
        except:
            tb = sys.exc_info()[2]
            import pdb
            pdb.post_mortem(tb)

class Filter(wx.Panel):
    def __init__(self, parent, name):
        wx.Panel.__init__(self, parent)
        self.name = name
        self._clauses = []

        box = wx.StaticBox(self, label=name)

        self.sizer = wx.StaticBoxSizer(box, wx.VERTICAL)
        self.button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        channels_button = wx.Button(self, label='Define channels')
        sort_text = wx.StaticText(self, label='Order by:')
        self.sort_choice = wx.Choice(self, choices=['Pathname', 'Filename'])
        self.button_sizer.AddSpacer(10)
        self.button_sizer.Add(channels_button)
        self.button_sizer.AddStretchSpacer()
        self.button_sizer.Add(sort_text, flag=wx.ALIGN_BOTTOM)
        self.button_sizer.Add(self.sort_choice)
        self.sizer.Add(self.button_sizer, proportion=1, flag=wx.EXPAND)
        self.SetSizer(self.sizer)

        self.add_after_clause(None)  # insert a single clause

    def add_after_clause(self, clause):
        new_clause = Clause(self)
        if len(self._clauses) > 0:
            new_idx = self._clauses.index(clause) + 1
        else:
            new_idx = 0
        self._clauses.insert(new_idx, new_clause)
        self.sizer.Insert(new_idx, new_clause, flag=wx.EXPAND | wx.ALL, border=2)
        for clause in self._clauses:
            clause.show_remove_button(len(self._clauses) > 1)
        self.Parent.Layout()
        self.Parent.Refresh()

    def remove_clause(self, clause):
        self._clauses.remove(clause)
        self.sizer.Remove(clause)
        clause.Destroy()
        for clause in self._clauses:
            clause.show_remove_button(len(self._clauses) > 1)
        self.Parent.Layout()
        self.Parent.Refresh()

    def set_focus(self, focused_clause):
        file_list = self.Parent.get_unfiltered_files()
        for clause in self._clauses:
            file_list = clause.filter(file_list)
            if clause == focused_clause:
                break
        self.Parent.set_filtered_list(file_list, 'After %s, clause %d' % (self.name, self._clauses.index(focused_clause) + 1))

class FilterSet(wx.Panel):
    def __init__(self, parent, name):
        wx.Panel.__init__(self, parent)
        self.name = name
        self._filters = []

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        add_button = wx.Button(self, label='Add new filter')
        self.sizer.Add(add_button, flag=wx.ALIGN_LEFT)
        self.SetSizer(self.sizer)

        add_button.Bind(wx.EVT_BUTTON, self.add_filter)

    def add_filter(self, evt):
        new_filter = Filter(self, 'filter %d' % (len(self._filters) + 1))
        self._filters.append(new_filter)
        self.sizer.Insert(len(self._filters) - 1, new_filter, flag=wx.EXPAND | wx.ALL, border=2)
        self.Layout()
        self.Refresh()

    def get_unfiltered_files(self):
        return self.Parent.get_unfiltered_files()

    def set_filtered_list(self, filtered_list, feedback):
        self.Parent.set_filtered_list(filtered_list, feedback)

class FileSource(wx.Panel):
    def __init__(self, parent, path):
        wx.Panel.__init__(self, parent)
        self._file_list = None
        self.path = path

        self.sizer = wx.BoxSizer(wx.HORIZONTAL)
        pathtext = wx.StaticText(self, label=path)
        pathtext.BackgroundColour = 'white'
        remove_button = wx.Button(self, label='-', style=wx.BU_EXACTFIT)
        self.sizer.Add(pathtext, proportion=1, flag=wx.ALL, border=1)
        self.sizer.Add(remove_button, flag=wx.BOTTOM, border=2)

        self.SetSizer(self.sizer)
        remove_button.Bind(wx.EVT_BUTTON, self.remove)

    def remove(self, evt):
        self.Parent.remove_source(self)

    def force_update(self):
        self._file_list = None
        self.get_files()

    def get_files(self):
        if self._file_list is not None:
            return self._file_list
        def genfiles():
            for path, dirnames, filenames in os.walk(self.path):
                for f in filenames:
                    yield os.path.join(path, f)
        return genfiles()


class FileSources(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        self._sources = []
        self.sources_to_images = OrderedDict()

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        add_button = wx.Button(self, label='+', style=wx.BU_EXACTFIT)
        self.sizer.Add(wx.StaticText(parent=self, label='File sources:'))
        self.sizer.Add(add_button, flag=wx.ALIGN_BOTTOM | wx.ALIGN_LEFT | wx.LEFT, border=3)
        self.SetSizer(self.sizer)

        add_button.Bind(wx.EVT_BUTTON, self.add_source)

    def add_source(self, evt):
        # this should pop up a dialog allowing selection of a directory, a set
        # of subdirs, a CSV/XLS file, or an http URL
        dialog = wx.DirDialog(parent=wx.GetApp().TopWindow, message="Choose directory")
        if dialog.ShowModal() != wx.ID_OK:
            dialog.Destroy()
            return
        source = FileSource(self, dialog.GetPath())
        self._sources.append(source)
        self.sizer.Insert(len(self._sources), source, flag=wx.EXPAND | wx.ALL, border=1)
        def genfiles():
            for f in source.get_files():
                if os.path.splitext(f)[1] in SUPPORTED_IMAGE_EXTENSIONS:
                    for metadata in extract_metadata(f):
                        yield metadata
        self.sources_to_images[source] = [md for md in genfiles()]
        self.Parent.add_files(self.sources_to_images[source])
        self.Layout()
        self.Refresh()

    def remove_source(self, source):
        self._sources.remove(source)
        del self.sources_to_images[source]
        self.sizer.Remove(source)
        source.Destroy()
        self.Parent.set_files(sum(self.sources_to_images.values(), []))
        self.Layout()
        self.Refresh()

class CPImageSetBuilder(wx.Frame):
    def __init__(self, *args, **kwargs):
        kwargs["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwargs)

        self._files = []

        self.files = wx.html.HtmlWindow(parent=self)
        self.filters = FilterSet(self, 'filters')
        self.channels = wx.html.HtmlWindow(parent=self)
        self.feedback = wx.html.HtmlWindow(parent=self)
        self.sources = FileSources(parent=self)
        
        self.files.SetPage('files')
        self.channels.SetPage('channels')
        self.feedback.SetPage('feedback')

        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)
        filters_file_sizer = wx.BoxSizer(wx.VERTICAL)
        chan_feedback_sizer = wx.BoxSizer(wx.VERTICAL)
        filters_file_sizer.Add(self.filters, 1, flag=wx.EXPAND | wx.BOTTOM, border=3)
        filters_file_sizer.Add(self.files, 1, flag=wx.EXPAND)
        chan_feedback_sizer.Add(self.channels, 1, flag=wx.EXPAND)
        chan_feedback_sizer.Add(self.feedback, 1, flag=wx.EXPAND)
        self.topsizer.Add(self.sources, 1, flag=wx.EXPAND | wx.RIGHT, border=3)
        self.topsizer.Add(filters_file_sizer, 3, flag=wx.EXPAND)
        self.topsizer.Add(chan_feedback_sizer, 1, flag=wx.EXPAND | wx.LEFT, border=3)

        border = wx.BoxSizer()
        border.Add(self.topsizer, 1, wx.EXPAND | wx.ALL, 5)
        border.Layout()
        self.SetSizer(border)

        self.Layout()

    def update_files(self, title='Default list', new_list=None):
        if new_list is None:
            new_list = self._files
        # de generator list
        new_list = [md for md in new_list]

        all_metadata_keys = OrderedDict()  # preserve order
        for md in new_list:
            all_metadata_keys.update(md)
        # XXX- This should use some sort of hierarchical display for files vs. images.
        page = '<table border=1 cellspacing=0 cellpadding=2 rules=cols width="100%">\n'
        page += '<tr align=left>%s</tr>\n' % ''.join('<th><b>%s</b></th>' % k for k in all_metadata_keys)
        rows = sorted(['<tr>%s</tr>' % ''.join('<td>%s</td>' % md.get(k, '') for k in all_metadata_keys) for md in new_list])
        page += '\n'.join(rows)
        page += '</table>'

        # add header
        # XXX-this should probably compute duplicate images, rather than files.
        num_duplicates = len(self._files) - len(set(f['URL'] for f in self._files))
        duplicates_warning = '' if num_duplicates == 0 else (', <font color="red">%d duplicate images!</font>' % num_duplicates)
        page = '<b>%s</b>, %d images%s\n' % (title, len(rows), duplicates_warning) + page
        self.files.SetPage(page)
        self.files.Refresh()

    def get_unfiltered_files(self):
        return self._files

    def set_filtered_list(self, new_list, title):
        self.update_files(title, new_list)

    def set_files(self, new_files):
        self._files = new_files
        self.update_files()

    def add_files(self, new_files):
        self._files += new_files
        self.update_files()


class MyApp(wx.App):
    def OnInit(self):
        frame = CPImageSetBuilder(None, title="Imageset builder")
        frame.Show(True)
        frame.update_files()
        self.SetTopWindow(frame)
        return True

app = MyApp(0)
app.MainLoop()
