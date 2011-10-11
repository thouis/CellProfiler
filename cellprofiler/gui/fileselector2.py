import wx
import wx.html
import os
import re
import time

try:
    from collections import OrderedDict
except:
    # http://pypi.python.org/pypi/ordereddict
    from ordereddict import OrderedDict

def substring_filter(val):
    return lambda s: val in s

def regexp_filter(val):
    pat = re.compile(val)
    return lambda s: pat.search(s)

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
        self.metadata_col = wx.Choice(self, choices=['Pathname', 'Filename'])
        self.filter_choice = wx.Choice(self, choices=self.FILTER_CHOICES.keys())
        self.value = wx.TextCtrl(self, value='', style=wx.TE_PROCESS_ENTER)
        cross_mark = wx.ArtProvider.GetBitmap(wx.ART_CROSS_MARK, wx.ART_MENU)
        remove_button = wx.Button(self, label='x', style=wx.BU_EXACTFIT)

        self.sizer.Add(self.metadata_col)
        self.sizer.AddSpacer(2)
        self.sizer.Add(self.filter_choice)
        self.sizer.AddSpacer(2)
        self.sizer.Add(self.value, proportion=1)
        self.sizer.AddSpacer(1)
        self.sizer.Add(remove_button)
        self.SetSizer(self.sizer)

        self.metadata_col.Bind(wx.EVT_CHOICE, self.update)
        self.filter_choice.Bind(wx.EVT_CHOICE, self.update)
        self.value.Bind(wx.EVT_TEXT_ENTER, self.update)
        self.value.Bind(wx.EVT_SET_FOCUS, self.set_focus)
        remove_button.Bind(wx.EVT_BUTTON, self.remove)

    def update(self, evt):
        filter_choice = self.filter_choice.GetStringSelection()
        self.sizer.Show(self.value, show=self.FILTER_CHOICES[filter_choice].get('takes_value', True))
        self.set_focus(evt)
        self.Layout()
        self.Refresh()

    def set_focus(self, evt):
        self.Parent.set_focus(self)

    def remove(self, evt):
        self.Parent.remove_clause(self)

    def filter(self, filelist):
        filter_info = self.FILTER_CHOICES[self.filter_choice.StringSelection]
        filt = filter_info['filter'](self.value.Value)
        inverted = filter_info.get('inverted', False)
        idx = 0 if (self.metadata_col.StringSelection == 'Pathname') else 1
        for pf in filelist:
            if (inverted and not filt(pf[idx])) or (filt(pf[idx]) and not inverted):
                yield pf

class Filter(wx.Panel):
    def __init__(self, parent, name):
        wx.Panel.__init__(self, parent)
        self.name = name
        self._clauses = []

        box = wx.StaticBox(self, label=name)
        box.BackgroundColour = 'blue'

        self.sizer = wx.StaticBoxSizer(box, wx.VERTICAL)
        self.button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        add_button = wx.Button(self, label='Add clause')
        channels_button = wx.Button(self, label='Define channels')
        sort_text = wx.StaticText(self, label='Order by:')
        self.sort_choice = wx.Choice(self, choices=['Pathname', 'Filename'])
        self.button_sizer.AddSpacer(10)
        self.button_sizer.Add(add_button)
        self.button_sizer.AddSpacer(5)
        self.button_sizer.Add(channels_button)
        self.button_sizer.AddStretchSpacer()
        self.button_sizer.Add(sort_text)
        self.button_sizer.Add(self.sort_choice)
        self.sizer.Add(self.button_sizer, proportion=1, flag=wx.EXPAND)
        self.SetSizer(self.sizer)

        add_button.Bind(wx.EVT_BUTTON, self.add_clause)

    def add_clause(self, evt):
        new_clause = Clause(self)
        self._clauses.append(new_clause)
        self.sizer.Insert(len(self.sizer.Children) - 1, new_clause, flag=wx.EXPAND | wx.ALL, border=2)
        self.Parent.Layout()
        self.Parent.Refresh()

    def remove_clause(self, clause):
        self._clauses.remove(clause)
        self.sizer.Remove(clause)
        clause.Destroy()
        self.Parent.Layout()
        self.Parent.Refresh()

    def set_focus(self, focused_clause):
        start_time = time.time()
        file_list = self.Parent.get_unfiltered_files()
        for clause in self._clauses:
            file_list = clause.filter(file_list)
            if clause == focused_clause:
                break
        self.Parent.set_filtered_list(file_list, self, focused_clause)

class FilterSet(wx.Panel):
    def __init__(self, parent, name):
        wx.Panel.__init__(self, parent)
        self.BackgroundColour = 'red'
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

    def set_filtered_list(self, filtered_list, a, b):
        self.Parent.set_filtered_list(filtered_list)

class CPImageSetBuilder(wx.Frame):
    def __init__(self, *args, **kwargs):
        kwargs["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwargs)

        self.files = wx.html.HtmlWindow(parent=self)
        self.filters = FilterSet(self, 'filters')
        self.channels = wx.html.HtmlWindow(parent=self)
        self.feedback = wx.html.HtmlWindow(parent=self)
        self.sources = wx.html.HtmlWindow(parent=self)
        
        self.files.SetPage('files')
        self.channels.SetPage('channels')
        self.feedback.SetPage('feedback')
        self.sources.SetPage('file sources')

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

    def update_files(self, new_list=None):
        def genfiles():
            for path, dirs, files in os.walk('.'):
                for f in files:
                    yield (path, f)
        if new_list is None:
            self._files = [(p, f) for (p, f) in genfiles()]
            new_list = self._files
        self.files.SetPage('<br>\n'.join(sorted([f for p, f in new_list])))
        self.files.Refresh()

    def get_unfiltered_files(self):
        return self._files

    def set_filtered_list(self, new_list):
        self.update_files(new_list)

class MyApp(wx.App):
    def OnInit(self):
        frame = CPImageSetBuilder(None, title="Imageset builder")
        frame.Show(True)
        frame.update_files()
        self.SetTopWindow(frame)
        return True

app = MyApp(0)
app.MainLoop()
