"""
@summary: Reports collected metrics
@author: CJ Grady
@version: 1.0
@status: alpha

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
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from StringIO import StringIO

SENDER = "lifemapper@ku.edu"
SMTP_SERVER = "smtp.ku.edu"

DAILY_RECEIVERS = ["cjgrady@ku.edu", "aimee.stewart@ku.edu"]
WEEKLY_RECEIVERS = ["cjgrady@ku.edu", "aimee.stewart@ku.edu", "beach@ku.edu"]
MONTHLY_RECEIVERS = ["cjgrady@ku.edu", "aimee.stewart@ku.edu", "beach@ku.edu"]

DAYS_OF_WEEK = [
                'Sunday', 
                'Monday', 
                'Tuesday', 
                'Wednesday', 
                'Thursday', 
                'Friday', 
                'Saturday', 
                'Sunday'
               ]

# .............................................................................
def reportIPaddresses(metrics, allFn, ctFn):
   with open(allFn, 'w') as f:
      for ip in metrics.ips:
         f.write("%s\n" % ip)
   
   with open(ctFn, 'w') as f:
      for ip in metrics.ct.keys():
         f.write("%s\n" % ip)

# .............................................................................
def getCoreMetrics(metrics):
   """
   @summary: Get core metrics for the date interval
   @param metrics: Metrics object to pull information out of
   """
   values = {
      "Traffic" : {
         "Number of Requests" : "{:,}".format(metrics.numRequests),
         "Number of Non-job Requests" : "{:,}".format(metrics.numRequests - metrics.numJobRequests),
         "Number of Change Thinking Requests" : "{:,}".format(metrics.numCTRequests),
         "Number of Map Requests" : "{:,}".format(metrics.numMapRequests),
         "Number of Bytes Sent" : metrics.getVolumeServedHuman(metrics.numBytesSent)
      },
      "Visitors" : {
         "Unique IP addresses" : "{:,}".format(len(metrics.ips)),
         "Unique CT IP addresses" : "{:,}".format(len(metrics.ct.keys())),
         "Number of CT 'sessions' (determined by the maximum number of visits to a url from a CT IP address)" : "{:,}".format(metrics.numCTsessions),
         "Number User Logins" : "{:,}".format(metrics.numLogins),
         "Number of Unique User Logins" : "{:,}".format(len(metrics.logins))
      },
      "Services" : {
         "Number of service calls" : "{:,}".format(metrics.serviceCalls['total']),
         # RAD
         "Number of RAD service calls" : "{:,}".format(metrics.serviceCalls['rad']['total']),
         "Number of RAD bucket POSTs" : "{:,}".format(metrics.serviceCalls['rad']['posts']['buckets']),
         "Number of RAD experiment POSTs" : "{:,}".format(metrics.serviceCalls['rad']['posts']['experiments']),
         "Number of RAD layer POSTs" : "{:,}".format(metrics.serviceCalls['rad']['posts']['layers']),
         # SDM
         "Number of SDM service calls" : "{:,}".format(metrics.serviceCalls['sdm']['total']),
         "Number of SDM experiment POSTs" : "{:,}".format(metrics.serviceCalls['sdm']['posts']['experiments']),
         "Number of SDM layer POSTs" : "{:,}".format(metrics.serviceCalls['sdm']['posts']['layers']),
         "Number of SDM occurrence set POSTs" : "{:,}".format(metrics.serviceCalls['sdm']['posts']['occurrences']),
         "Number of SDM scenario POSTs" : "{:,}".format(metrics.serviceCalls['sdm']['posts']['scenarios']),
      },
      "HTTP Status Codes" : metrics.statusCounts,
      "HTTP Methods" : metrics.methodsCounts
   }
   
   html = []
   alternative = []
   for key in values:
      html.append('<p>')
      html.append('   <h3>{}:</h3>'.format(key))
      html.append('   <ul>')
      alternative.append('{}:'.format(key))
      
      for k in values[key].keys():
         html.append('      <li><b>{0}:</b>&nbsp;{1}<br /></li>'.format(k, values[key][k]))
         alternative.append('  {0}: {1}'.format(k, values[key][k]))
      
      html.append('   </ul>')
      html.append('</p>')
      alternative.append('')
   
   return '\n'.join(html), '\n'.join(alternative)

# .............................................................................
def getDailyMessage(metrics):
   """
   @summary: Get a daily report and send it as an email
   """
   bkt = metrics.nonJobBuckets
   
   hourlyData = []
   
   for year in sorted(bkt.keys()):
      for month in sorted(bkt[year].keys()):
         for day in sorted(bkt[year][month].keys()):
            for hour in sorted(bkt[year][month][day].keys()):
               try:
                  hrTotal = bkt[year][month][day][hour]
                  hourlyData.append(hrTotal)
               except:
                  hourlyData.append(0)
   # Plot 2 - Hourly breakdown
   plt.plot(hourlyData)
   plt.xticks(range(0, 24), range(0, 24))
   plt.xlabel("Hour of day")
   plt.ylabel("Number of web hits")
   plt.title("Non-job hits per hour")
   sHourly = StringIO()
   plt.savefig(sHourly, format='png')
   plt.clf()
   
   msgRoot = MIMEMultipart('related')
   msgRoot['Subject'] = "Daily Lifemapper metrics (%s/%s/%s)" % \
                               (metrics.startMonth, metrics.startDay, 
                                metrics.startYear)
   msgRoot['From'] = SENDER
   msgRoot['To'] = ', '.join(DAILY_RECEIVERS)
   
   baseHtml, alternate = getCoreMetrics(metrics)
   
   msgAlternative = MIMEMultipart('alternative')
   msgRoot.attach(msgAlternative)
   msgText = MIMEText(alternate)
   msgAlternative.attach(msgText)
   
   msgText = MIMEText('{0}{1}'.format(baseHtml, 
                           '<p>Hourly hits</p><img src="cid:image1" /><br />'), 
                      'html')
   msgAlternative.attach(msgText)
   
   msgImage2 = MIMEImage(sHourly.getvalue())
   
   msgImage2.add_header("Content-ID", "<image1>")
   
   msgRoot.attach(msgImage2)
   
   _sendMessage(SENDER, DAILY_RECEIVERS, msgRoot.as_string())

# .............................................................................
def getWeeklyMessage(metrics):
   """
   @summary: Get a weekly report and send it as an email
   """
   bkt = metrics.nonJobBuckets
   
   dailyData = []
   days = []
   hourlyData = []
   
   for year in sorted(bkt.keys()):
      for month in sorted(bkt[year].keys()):
         for day in sorted(bkt[year][month].keys()):
            dayTotal = 0
            for hour in sorted(bkt[year][month][day].keys()):
               try:
                  hrTotal = bkt[year][month][day][hour]
                  dayTotal += hrTotal
                  hourlyData.append(hrTotal)
               except:
                  hourlyData.append(0)
            dailyData.append(dayTotal)
            days.append(DAYS_OF_WEEK[datetime.date(year, month, day).isoweekday()])
   # Plot 1 - Daily breakdown
   plt.plot(dailyData)
   plt.xticks(range(7), days)
   plt.xlabel("Day of week")
   plt.ylabel("Number of web hits")
   plt.title("Non-job hits per day")
   sDaily = StringIO()
   plt.savefig(sDaily, format='png')
   plt.clf()
   
   # Plot 2 - Hourly breakdown
   plt.plot(hourlyData)
   plt.xticks(range(0, len(hourlyData), 6), (len(hourlyData)/4) * [0, 6, 12, 18])
   plt.xlabel("Hour of day")
   plt.ylabel("Number of web hits")
   plt.title("Non-job hits per hour")
   sHourly = StringIO()
   plt.savefig(sHourly, format='png')
   plt.clf()
   
   msgRoot = MIMEMultipart('related')
   msgRoot['Subject'] = "Weekly Lifemapper metrics (%s/%s/%s - %s/%s/%s)" % \
                               (metrics.startMonth, metrics.startDay, 
                                metrics.startYear, metrics.endMonth, 
                                metrics.endDay, metrics.endYear)
   msgRoot['From'] = SENDER
   msgRoot['To'] = ', '.join(WEEKLY_RECEIVERS)
   
   baseHtml, alternate = getCoreMetrics(metrics)
   
   msgAlternative = MIMEMultipart('alternative')
   msgRoot.attach(msgAlternative)
   msgText = MIMEText(alternate)
   msgAlternative.attach(msgText)
   
   msgText = MIMEText('{0}{1}{2}'.format(baseHtml, 
                           '<p>Daily hits</p><img src="cid:image1" /><br />', 
                           '<p>Hourly hits</p><img src="cid:image2" /><br />'), 
                      'html')
   msgAlternative.attach(msgText)
   
   msgImage1 = MIMEImage(sDaily.getvalue())
   msgImage2 = MIMEImage(sHourly.getvalue())
   
   msgImage1.add_header("Content-ID", "<image1>")
   msgImage2.add_header("Content-ID", "<image2>")
   
   msgRoot.attach(msgImage1)
   msgRoot.attach(msgImage2)
   
   _sendMessage(SENDER, WEEKLY_RECEIVERS, msgRoot.as_string())

# .............................................................................
def getMonthlyMessage(metrics):
   """
   @summary: Gets a monthly report and sends it as an email
   """
   bkt = metrics.nonJobBuckets
   
   dailyData = []
   days = {
           1: [],
           2: [],
           3: [],
           4: [],
           5: [],
           6: [],
           7: []
          }
   
   for year in sorted(bkt.keys()):
      for month in sorted(bkt[year].keys()):
         for day in sorted(bkt[year][month].keys()):
            dayTotal = 0
            for hour in sorted(bkt[year][month][day].keys()):
               try:
                  hrTotal = bkt[year][month][day][hour]
                  dayTotal += hrTotal
               except:
                  pass
            dailyData.append(dayTotal)
            days[datetime.date(year, month, day).isoweekday()].append(dayTotal)
   
   # Plot 1 - Daily breakdown
   plt.plot(dailyData)
   #plt.xticks(range(7), days)
   plt.xlabel("Day of month")
   plt.ylabel("Number of web hits")
   plt.title("Non-job hits per day")
   sDaily = StringIO()
   plt.savefig(sDaily, format='png')
   plt.clf()
   
   # Plot 2 - Hourly breakdown
   mins = []
   maxs = []
   avgs = []
   for i in xrange(1, 8):
      mins.append(min(days[i]))
      maxs.append(max(days[i]))
      avgs.append(sum(days[i])/len(days[i]))
   plt.plot(mins, label='Minimum')
   plt.plot(maxs, label='Maximum')
   plt.plot(avgs, label='Average')
   plt.xticks(xrange(7), DAYS_OF_WEEK)
   plt.xlabel("Day of week")
   plt.ylabel("Number of web hits")
   plt.title("Non-job hits per day")
   plt.legend()
   sSummary = StringIO()
   plt.savefig(sSummary, format='png')
   plt.clf()
   
   msgRoot = MIMEMultipart('related')
   msgRoot['Subject'] = "Monthly Lifemapper metrics (%s/%s/%s - %s/%s/%s)" % \
                               (metrics.startMonth, metrics.startDay, 
                                metrics.startYear, metrics.endMonth, 
                                metrics.endDay, metrics.endYear)
   msgRoot['From'] = SENDER
   msgRoot['To'] = ', '.join(MONTHLY_RECEIVERS)
   
   baseHtml, alternate = getCoreMetrics(metrics)
   
   msgAlternative = MIMEMultipart('alternative')
   msgRoot.attach(msgAlternative)
   msgText = MIMEText(alternate)
   msgAlternative.attach(msgText)
   
   msgText = MIMEText('{0}{1}{2}'.format(baseHtml, 
                           '<p>Daily hits</p><img src="cid:image1" /><br />', 
                           '<p>Summary hits</p><img src="cid:image2" /><br />'), 
                      'html')
   msgAlternative.attach(msgText)
   
   msgImage1 = MIMEImage(sDaily.getvalue())
   msgImage2 = MIMEImage(sSummary.getvalue())
   
   msgImage1.add_header("Content-ID", "<image1>")
   msgImage2.add_header("Content-ID", "<image2>")
   
   msgRoot.attach(msgImage1)
   msgRoot.attach(msgImage2)
   
   _sendMessage(SENDER, MONTHLY_RECEIVERS, msgRoot.as_string())

# .............................................................................
def _sendMessage(sender, receivers, msg):
   """
   @summary: Sends an email message
   """
   import smtplib
   smtp = smtplib.SMTP(SMTP_SERVER)
   smtp.sendmail(sender, receivers, msg)

