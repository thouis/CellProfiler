#!/usr/bin/env /imaging/analysis/People/imageweb/batchprofiler/cgi-bin/development/python-2.6.sh
#
# Kill all jobs in a batch
#
print "Content-Type: text/html"
print
import cgitb
cgitb.enable()
import RunBatch
import cgi
import sys

form = cgi.FieldStorage()
if form.has_key("job_id"):
    import subprocess
    job_id = int(form["job_id"].value)
    run = {"job_id":job_id}
    RunBatch.KillOne(run)
    print"""
    <html><head><title>Job %(job_id)d killed</title></head>
    <body>Job %(job_id)d killed
    </body>
    </html>
"""%(globals())
    sys.exit(0)
batch_id = int(form["batch_id"].value)
my_batch = RunBatch.LoadBatch(batch_id)
for run in my_batch["runs"]:
    RunBatch.KillOne(run)

url = "ViewBatch.py?batch_id=%(batch_id)d"%(my_batch)
print "<html><head>"
print "<meta http-equiv='refresh' content='0; URL=%(url)s' />"%(globals())
print "</head>"
print "<body>This page should be redirected to <a href='%(url)s'/>%(url)s</a></body>"%(globals())
print "</html>"
