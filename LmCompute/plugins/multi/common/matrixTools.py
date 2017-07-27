"""
@summary: Module containing common matrix related tools
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import numpy
from StringIO import StringIO
import urllib2

# .......................................
def getNumpyMatrixFromCSV(csvFn=None, csvUrl=None):
   """
   @summary: Retrieves a csv from a url and then converts that csv file into 
                a Numpy matrix
   @param csvFn: (optional) A filename for a CSV file
   @param csvUrl: (optional) A URL to a CSV file
   @note: Either csvFn or csvUrl must be specified (csvFn takes priority)
   """
   csvContent = StringIO()

   if csvFn is None:
      if csvUrl is None:
         raise Exception, "Must provide either a filename or url"
      else:
         csvContent.write(urllib2.urlopen(csvUrl).read())
   else:
      csvContent.write(open(csvFn, 'r').read()) 

   csvContent.seek(0)

   # assume no header
   ary = numpy.loadtxt(csvContent, dtype=numpy.int, delimiter=',', skiprows=0)
   return ary

#    sample = csvContent.read(1024)
#    csvContent.seek(0)
#    sniffer = csv.Sniffer()
# 
#    dialect = sniffer.sniff(sample)
#    hasHeaders = sniffer.has_header(sample)
#    
#    csvReader = csv.reader(csvContent, dialect=dialect, lineterminator='\n')
# 
#    s = ';'.join([' '.join([c for c in r]) for r in csvReader][int(hasHeaders):])
#    matrix = numpy.matrix(s)

#   return matrix
