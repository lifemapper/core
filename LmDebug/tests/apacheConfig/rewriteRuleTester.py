"""
@summary: This script tests that the rewrite rules are set up correctly in the
             Apache configuration
@note: This can test a server configured with .htaccess or in Apache site conf
"""
import argparse
import urllib2

WEBSITE_ROOT = "http://dev.lifemapper.org"
REWRITE_ROOT = "http://dev.lifemapper.org"
FORWARD_ROOT = "http://svc.lifemapper.org"

# These URLs stay at the same host but have a new URL
REWRITE_URLS = [
   ("/species", "/?page_id=863"),
   ("/species/", "/?page_id=863"),
   ("/species/Acer", "/?page_id=863&speciesname=Acer")
]

# These URLs are kept the same, just forwarded to a different host
FORWARD_URLS = [
   "/clients/algorithms.xml",
   "/clients/instances.xml",
   "/clients/versions.xml",
   "/css/services.xsl",
   "/hint/species/bre?maxReturned=60&format=json",
   "/jobs?request=existJobs&jobTypes=110",
   "/login",
   ("/logout", "/login"), # Logout forwards back to login
   "/schemas/serviceResponse.xsd",
   "/signup",
   "/services",
   "/services/sdm",
   "/services/rad",
   "/services/sdm/layers",
   "/services/sdm/scenarios",
   "/spLinks",
   "/robots.txt",
]

# .............................................................................
def checkUrl(origUrl, redirectUrl):
   """
   @summary: Checks that the URL redirects correctly
   """
   print "Checking", origUrl
   res = urllib2.urlopen(origUrl)
   if res.code >= 400:
      raise Exception, "There was a problem with the response from %s" % origUrl
   if res.url != redirectUrl:
      raise Exception, "URL not rewritten properly, %s != %s" % (res.url, redirectUrl)

# .............................................................................
if __name__ == "__main__":
   urls = []
   for oUrl, rUrl in REWRITE_URLS:
      urls.append(("%s%s" % (WEBSITE_ROOT, oUrl), "%s%s" % (REWRITE_ROOT, rUrl)))
   
   for fUrl in FORWARD_URLS:
      if isinstance(fUrl, tuple):
         urls.append(("%s%s" % (WEBSITE_ROOT, fUrl[0]), "%s%s" % (FORWARD_ROOT, fUrl[1])))
      else:
         urls.append(("%s%s" % (WEBSITE_ROOT, fUrl), "%s%s" % (FORWARD_ROOT, fUrl)))
   
   for oUrl, rUrl in urls:
      checkUrl(oUrl, rUrl)
