"""
@summary: Module containing classes for dealing with unicode CSVs
@note: This doesn't seem like our coding style, I think it was found somewhere
          and added for the Stinkbait PoC
@author: CJ Grady
@version: 
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import codecs
import cStringIO
import csv

from LmCommon.common.lmconstants import ENCODING

# ............................................................................
class UTF8Recoder(object):
   """
   @summary: Iterator that reads an encoded stream and re-encodes the input to 
                the encoding
   """
   # ..............................
   def __init__(self, f, encoding):
      self.reader = codecs.getreader(encoding)(f)

   # ..............................
   def __iter__(self):
      return self

   # ..............................
   def next(self):
      return self.reader.next().encode(ENCODING)

# ............................................................................
class UnicodeReader(object):
   """
   @summary: A CSV reader which will iterate over lines in the CSV file "f",
               which is encoded in the given encoding.
   """

   # ..............................
   def __init__(self, f, dialect=csv.excel, encoding=ENCODING, **kwds):
      f = UTF8Recoder(f, encoding)
      self.reader = csv.reader(f, dialect=dialect, **kwds)

   # ..............................
   def next(self):
      row = self.reader.next()
      return [unicode(s, ENCODING) for s in row]

   # ..............................
   def __iter__(self):
      return self

# ............................................................................
class UnicodeWriter(object):
   """
   @summary: A CSV writer which will write rows to CSV file "f", which is 
                encoded in the given encoding.
   """
   # ..............................
   def __init__(self, f, dialect=csv.excel, encoding=ENCODING, **kwds):
      # Redirect output to a queue
      self.queue = cStringIO.StringIO()
      self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
      self.stream = f
      self.encoder = codecs.getincrementalencoder(encoding)()

   # ..............................
   def writerow(self, row):
      self.writer.writerow([s.encode(ENCODING) for s in row])
      # Fetch UTF-8 output from the queue ...
      data = self.queue.getvalue()
      data = data.decode(ENCODING)
      # ... and reencode it into the target encoding
      data = self.encoder.encode(data)
      # write to the target stream
      self.stream.write(data)
      # empty queue
      self.queue.truncate(0)

   # ..............................
   def writerows(self, rows):
      for row in rows:
         self.writerow(row)
