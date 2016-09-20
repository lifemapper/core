"""
@summary: Collects usage metrics by analyzing Apache logs
@author: CJ Grady
@version: 1.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import datetime
import glob
import gzip
import hashlib
import os
import sys
import time

from LmWebServer.scripts.metrics.reportMetrics import (getDailyMessage,
                         getWeeklyMessage, getMonthlyMessage, reportIPaddresses)

NUM_DAYS = {
            1 : 31,
            2 : 28,
            3 : 31,
            4 : 30,
            5 : 31,
            6 : 30,
            7 : 31,
            8 : 81,
            9 : 30,
            10: 31,
            11: 30,
            12: 31
           }

MONTHS = {
          'Jan' : 1,
          'Feb' : 2,
          'Mar' : 3,
          'Apr' : 4,
          'May' : 5,
          'Jun' : 6,
          'Jul' : 7,
          'Aug' : 8,
          'Sep' : 9,
          'Oct' : 10,
          'Nov' : 11,
          'Dec' : 12
         }

APACHE_LOG_PATH = "/var/log/apache2/"

DAY_INTERVAL = "day"
WEEK_INTERVAL = "week"
MONTH_INTERVAL = "month"
MONTH_IP_INTERVAL = "monthip"

usage = "Usage: (%s | %s | %s | %s) [offset]" % (DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, MONTH_IP_INTERVAL)

# .............................................................................
class RequestTime(object):
   # ....................................
   def __init__(self, text):
      timeParts = text.split('/')
      self.day = int(timeParts[0])
      self.month = MONTHS[timeParts[1]]
      timeParts2 = timeParts[2].split(':')
      self.year = int(timeParts2[0])
      self.hour = int(timeParts2[1])
      self.minute = int(timeParts2[2])
      self.text = text
   
   # ....................................
   def __str__(self):
      return self.text

# .............................................................................
class Request(object):
   # ....................................
   def __init__(self, logLine):
      partsBySpace = logLine.split(' ')
      partsByQuote = logLine.split('"')
      
      self.host = partsBySpace[0] # IP
      self.remoteLogName = partsBySpace[1]
      self.remoteUser = partsBySpace[2]
      self.time = RequestTime(logLine.split('[')[1].split(']')[0])

      temp = partsByQuote[2].strip().split(' ')
      self.status = int(temp[0])
      self.bytesSent = int(temp[1])

      self.referer = partsByQuote[3]
      self.userAgent = partsByQuote[5]
      
      # Process request
      unprocessedRequest = partsByQuote[1]
      requestParts = unprocessedRequest.split(' ')
      if len(requestParts) < 3:
         self.method = None
         self.endPoint = None
         self.HTTPVersion = None
      else:
         self.method = requestParts[0]
         self.endPoint = requestParts[1]
         self.HTTPVersion = requestParts[2]

# .............................................................................
class MetricsCollector(object):
   # ....................................
   def __init__(self):
      self.numRequests = 0
      self.ips = set()
      self.numBytesSent = 0
      self.numLogins = 0
      self.logins = set()
      self.statusCounts = {}
      self.methodsCounts = {}
      self.startYear = None
      self.startMonth = None
      self.startDay = None
      self.endYear = None
      self.endMonth = None
      self.endDay = None
      
      self.numCTsessions = 0
      self.numJobRequests = 0
      self.numCTRequests = 0
      self.numMapRequests = 0
      
      self.serviceCalls = {
                           'total' : 0,
                           'rad' : {
                              'total' : 0,
                              'posts' : {
                                 'buckets' : 0,
                                 'experiments' : 0,
                                 'layers' : 0
                              }
                           },
                           'sdm' : {
                              'total' : 0,
                              'posts' : {
                                'experiments' : 0,
                                'layers' : 0,
                                'occurrences' : 0,
                                'scenarios' : 0
                              }
                           }
                          }
      
      self.ct = {}
      
      self.buckets = {}
      self.jobBuckets = {}
      self.nonJobBuckets = {}
      self.ctBuckets = {}
      
   # ....................................
   def setStartTime(self, year, month, day):
      self.startYear = year
      self.startMonth = month
      self.startDay = day
      
   # ....................................
   def setEndTime(self, year, month, day):
      self.endYear = year
      self.endMonth = month
      self.endDay = day
   
   # ....................................
   def collectFromLog(self, fn):
      print "Adding", fn
      with open(fn) as f:
         self._collectFromFLO(f)
   
   # ....................................
   def collectFromZip(self, fn):
      print "Adding", fn
      with gzip.open(fn) as f:
         self._collectFromFLO(f)
   
   # ....................................
   def _inTimeRange(self, t):
      afterStart = True
      beforeEnd = True
      # Greater than start time
      if self.startDay is not None and self.startMonth is not None and self.startYear is not None:
         afterStart = (self.startYear, self.startMonth, self.startDay) <= (t.year, t.month, t.day)
      # Less than end time
      if self.endDay is not None and self.endMonth is not None and self.endYear is not None:
         beforeEnd = (t.year, t.month, t.day) <= (self.endYear, self.endMonth, self.endDay)
      
      return afterStart and beforeEnd
   
   # ....................................
   def _incrementBucket(self, t, requests=True, jobs=False, nonJobs=False, ct=False):
      if requests:
         if not self.buckets.has_key(t.year):
            self.buckets[t.year] = {}
         if not self.buckets[t.year].has_key(t.month):
            self.buckets[t.year][t.month] = {}
         if not self.buckets[t.year][t.month].has_key(t.day):
            self.buckets[t.year][t.month][t.day] = {}
         if not self.buckets[t.year][t.month][t.day].has_key(t.hour):
            self.buckets[t.year][t.month][t.day][t.hour] = 0
         self.buckets[t.year][t.month][t.day][t.hour] += 1
      if jobs:
         if not self.jobBuckets.has_key(t.year):
            self.jobBuckets[t.year] = {}
         if not self.jobBuckets[t.year].has_key(t.month):
            self.jobBuckets[t.year][t.month] = {}
         if not self.jobBuckets[t.year][t.month].has_key(t.day):
            self.jobBuckets[t.year][t.month][t.day] = {}
         if not self.jobBuckets[t.year][t.month][t.day].has_key(t.hour):
            self.jobBuckets[t.year][t.month][t.day][t.hour] = 0
         self.jobBuckets[t.year][t.month][t.day][t.hour] += 1
      if nonJobs:
         if not self.nonJobBuckets.has_key(t.year):
            self.nonJobBuckets[t.year] = {}
         if not self.nonJobBuckets[t.year].has_key(t.month):
            self.nonJobBuckets[t.year][t.month] = {}
         if not self.nonJobBuckets[t.year][t.month].has_key(t.day):
            self.nonJobBuckets[t.year][t.month][t.day] = {}
         if not self.nonJobBuckets[t.year][t.month][t.day].has_key(t.hour):
            self.nonJobBuckets[t.year][t.month][t.day][t.hour] = 0
         self.nonJobBuckets[t.year][t.month][t.day][t.hour] += 1
      if ct:
         if not self.ctBuckets.has_key(t.year):
            self.ctBuckets[t.year] = {}
         if not self.ctBuckets[t.year].has_key(t.month):
            self.ctBuckets[t.year][t.month] = {}
         if not self.ctBuckets[t.year][t.month].has_key(t.day):
            self.ctBuckets[t.year][t.month][t.day] = {}
         if not self.ctBuckets[t.year][t.month][t.day].has_key(t.hour):
            self.ctBuckets[t.year][t.month][t.day][t.hour] = 0
         self.ctBuckets[t.year][t.month][t.day][t.hour] += 1
         
   # ....................................
   def _collectFromFLO(self, flo):
      for line in flo:
         try:
            req = Request(line)
            
            if self._inTimeRange(req.time):
               self.ips.add(req.host)
               self.numBytesSent = self.numBytesSent + req.bytesSent
               if self.statusCounts.has_key(req.status):
                  self.statusCounts[req.status] += 1
               else:
                  self.statusCounts[req.status] = 1
               
               if self.methodsCounts.has_key(req.method):
                  self.methodsCounts[req.method] += 1
               else:
                  self.methodsCounts[req.method] = 1
               
               self.numRequests += 1
               self._incrementBucket(req.time, requests=True)
               
               # Look at request endpoint
               if req.endPoint is not None:
                  if req.endPoint.startswith('/login'):
                     self.numLogins += 1
                     try:
                        usr = req.endPoint.split('username')[1].split('&')[0]
                     except:
                        usr = "Unknown"
                     self.logins.add(usr)
               
                  if req.endPoint.startswith('/ogc'):
                     self.numMapRequests += 1
                  
                  if req.endPoint.startswith('/services'):
                     self.serviceCalls['total'] += 1
                     if req.endPoint.find('rad') > 0:
                        self.serviceCalls['rad']['total'] += 1
                        if req.method.lower() == 'post':
                           if req.endPoint.lower().find('addbucket') > 0:
                              self.serviceCalls['rad']['posts']['buckets'] += 1
                           elif req.endPoint.find('experiments') > 0:
                              self.serviceCalls['rad']['posts']['experiments'] += 1
                           elif req.endPoint.find('layers') > 0:
                              self.serviceCalls['rad']['posts']['layers'] += 1
                     if req.endPoint.find('sdm') > 0:
                        self.serviceCalls['sdm']['total'] += 1
                        if req.method.lower() == 'post':
                           ep = req.endPoint
                           if ep.find('experiments') > 0:
                              self.serviceCalls['sdm']['posts']['experiments'] += 1
                           if ep.find('layers') > 0:
                              self.serviceCalls['sdm']['posts']['layers'] += 1
                           if ep.find('occurrences') > 0:
                              self.serviceCalls['sdm']['posts']['occurrences'] += 1
                           if ep.find('scenarios') > 0:
                              self.serviceCalls['sdm']['posts']['scenarios'] += 1
                  
                  if req.endPoint.startswith('/jobs'):
                     self.numJobRequests += 1
                     self._incrementBucket(req.time, requests=False, jobs=True)
                  else:
                     self._incrementBucket(req.time, requests=False, nonJobs=True)
               
               # Look at referer
               if req.referer.lower().find('changethinking') >= 0:
                  self.numCTRequests += 1
                  key = hashlib.md5(req.endPoint).hexdigest()
                  if not self.ct.has_key(req.host):
                     self.ct[req.host] = {}
                  if self.ct[req.host].has_key(key):
                     self.ct[req.host][key] += 1
                  else:
                     self.ct[req.host][key] = 1
                  self._incrementBucket(req.time, requests=False, nonJobs=True, ct=True)
            
         except Exception, e:
            print "Unable to process:", line
            print str(e)

      for ip in sorted(self.ct.keys()):
         m = 0
         for key in self.ct[ip].keys():
            if self.ct[ip][key] > m:
               m = self.ct[ip][key]
         self.numCTsessions += m
            
   # ....................................
   def getVolumeServedHuman(self, vol):
      SUFFIXES = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
      i = 0
      while vol >= 1024 and i < len(SUFFIXES):
         vol = vol / 1024.0
         i += 1
      return "%.3f %s" % (vol, SUFFIXES[i])

            
# .............................................................................
if __name__ == "__main__":
   offset = 0
   
   if len(sys.argv) == 1:
      print usage
      exit(0)
   else:
      interval = sys.argv[1].lower()
      if len(sys.argv) > 2:
         offset = int(sys.argv[2])
      
   # Get current day
   currTime = datetime.datetime.now()
   if interval == DAY_INTERVAL:
      st = et = currTime - offset * datetime.timedelta(1)
      func = getDailyMessage
   elif interval == WEEK_INTERVAL:
      st = currTime - (offset+1) * datetime.timedelta(7) - datetime.timedelta(1)
      et = st + datetime.timedelta(7)
      func = getWeeklyMessage
   elif interval in [MONTH_INTERVAL, MONTH_IP_INTERVAL]:
      startMonth = (currTime.month - offset) % 12
      if startMonth == 0:
         startMonth = 12
      startYear = currTime.year
      temp = offset - currTime.month
      while temp >= 12:
         startYear -= 1
         temp -= 12
      st = datetime.datetime(startYear, startMonth, 1)
      
      endYear = startYear
      endMonth = startMonth + 1
      if endMonth > 12:
         endMonth = 1
         endYear += 1
      et = datetime.datetime(endYear, endMonth, 1) - datetime.timedelta(1)
      if interval == MONTH_INTERVAL:
         func = getMonthlyMessage
      else:
         allFn = "allIPs%s.txt" % startMonth
         ctFn = "ctIPs%s.txt" % startMonth
         func = lambda x : reportIPaddresses(x, allFn, ctFn)
   
   print "Collecing Metrics"
   metrics = MetricsCollector()
   metrics.setStartTime(st.year, st.month, st.day)
   metrics.setEndTime(et.year, et.month, et.day)
   
   print "(%s-%s-%s) - (%s-%s-%s)" % (st.year, st.month, st.day, et.year, et.month, et.day)

   # Parse directory for log files to check
   for fn in glob.glob(os.path.join(APACHE_LOG_PATH, "access*")):
      print "Looking at:", fn
      t = time.localtime(os.stat(fn).st_mtime)
      mt = datetime.datetime(t.tm_year, t.tm_mon, t.tm_mday)
      #ct = datetime.datetime(t2.tm_year, t2.tm_mon, t2.tm_mday)
      
      if st <= mt:# and ct <= et + datetime.timedelta(1):
         if fn.endswith('gz'):
            metrics.collectFromZip(fn)
         else:
            metrics.collectFromLog(fn)
         
   # Run report
   func(metrics)
   
