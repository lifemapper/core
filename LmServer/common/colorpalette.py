#!/usr/bin/env python
"""
Module: colorpalette.py
Author: Dave Vieglais
Date Last Modified: 12 March 2005

Do "python colorpalette.py > colorpalette.html" for documentation and examples.

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
###################
#++ colorPalette ++

class colorPalette:
  """
  Implements a simple color palette generator.  Intended for use in
  providing a color scale for rendering continuous values.

  An instance of colorPalette may be treated like a list. Eg:
  p = colorPalette()
  for c in p:
    print c
  """

  def __init__(self,ptype='pretty',n=255,alpha=0):
    """
    ptype is the name of the palette to use:
      pretty, gray, red, green, blue, or safe.
    """
    self.pal = []
    if n > 0:
       self.n = n
    else:
       raise Exception('Number of bins (n) must be greater than 0')
    self.ptype = ptype
    self.alpha = alpha
    self.ptypes = {"gray":self.grayscale,
                   "red":self.redscale,
                   "green":self.greenscale,
                   "blue":self.bluescale,
                   "yellow":self.yellowscale,
                   "fuschia":self.fuschiascale, 
                   "aqua":self.aquascale,
                   "safe":self.safetycolors,
                   "pretty":self.prettyscale,
                   "bluered":self.blueredscale,
                   "bluegreen":self.bluegreenscale,
                   "greenred":self.greenredscale}
    if self.ptype not in list(self.ptypes.keys()):
      self.ptype = "gray"
    self.method = self.ptypes[self.ptype]
    self.method()

  def keys(self):
    return list(self.ptypes.keys())

  def __getitem__(self,key):
    return self.pal.__getitem__(key)

  def __len__(self):
    return self.pal.__len__()

  def __iter__(self):
    return self.pal.__iter__()

  def __contains__(self,item):
    return self.pal.__contains__()

  def __delitem__(self,key):
    return self.pal.__delitem__()

  def __setitem__(self,key,value):
    return self.pal.__setitem__(key,value)

  def grayscale(self):
    """
    Standard grayscale.
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([v,v,v,a])

  def redscale(self):
    """
    Reds from black to very red.
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([v,0,0,a])

  def greenscale(self):
    """
    Greens from black to green
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([0,v,0,a])

  def bluescale(self):
    """
    Blues from black to blue blue.
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([0,0,v,a])

  def yellowscale(self):
    """
    Yellows from black to yellow.
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([v,v,0,a])

  def fuschiascale(self):
    """
    Fuschias from black to fuschia.
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([v,0,v,a])

  def aquascale(self):
    """
    Aquas from black to aqua.
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      v = int(i*scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([0,v,v,a])

  def safetycolors(self):
    """
    Generates a palette of 125 "web safe" colors.
    Note that this palette does not provide a gradient suitable
    for rendering continuous values.
    """
    self.pal = []
    a = 0
    idx = 0
    for i in range(0,6):
      b = i*0x33
      for j in range(0,6):
        g = j*0x33
        for k in range(0,6):
          r = k*0x33
          if idx==self.alpha:
            a = 0
          else:
            a = 255
          self.pal.append([r,g,b,a])
          idx = idx + 1

  def prettyscale(self):
    """
    Makes a palette of appealing colors (to me anyway) for a continuous gradient.
    The colors and intensities are intended to maximize percieved separation
    of values across the range.
    """
    import math
    self.pal = []
    a = 0
    rscl = [0.0, -1.0, math.pi/(self.n*0.8)]
    gscl = [0.3, 0.7, math.pi/(self.n*1.0)]
    bscl = [0.0, 1.0, math.pi/(self.n*1.5)]
    for i in range(0,self.n+1):
      r = rscl[0] + rscl[1] * math.cos(i*rscl[2])
      g = gscl[0] + gscl[1] * math.sin(i*gscl[2])
      b = bscl[0] + bscl[1] * math.cos(i*bscl[2])
      if r < 0: r = 0
      elif r > 1.0: r = 1.0
      if g < 0: g = 0
      elif g > 1.0: g = 1.0
      if b < 0: b = 0
      elif b > 1.0: b = 1.0
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([int(r*255), int(g*255), int(b*255), a])

  def blueredscale(self):
    """
    Colors ranging from blue through to red
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      vr = int(i*scl)
      vb = int((self.n - i) * scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([vr,0,vb,a])

  def bluegreenscale(self):
    """
    Colors ranging from blue through to green
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      vg = int(i*scl)
      vb = int((self.n - i) * scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([0,vg,vb,a])

  def greenredscale(self):
    """
    Colors ranging from green through to red
    """
    self.pal = []
    a = 0
    scl = 255.0/(self.n * 1.0)
    for i in range(0, self.n+1):
      vr = int(i*scl)
      vg = int((self.n - i) * scl)
      if i == self.alpha:
        a = 0
      else:
        a = 255
      self.pal.append([vr,vg,0,a])


  def toHTMLTable(self):
    """
    Generates a table of palette colors
    """
    s = ""
    s += "<table border='1'>\n"
    s += "<tr> <th>Index</th> <th>R</th> <th>G</th> <th>B</th> <th>A</th> <th width='50%'>Color</th> </tr>\n"
    for i in range(0,len(self)):
      c = self.pal[i]
      s += "<tr>"
      s += "<td>%d</td> <td>%d</td> <td>%d</td> <td>%d</td> <td>%d</td>" % (i,c[0],c[1],c[2],c[3])
      s += "<td bgcolor='#%02X%02X%02X'>&nbsp;</td>" % (c[0],c[1],c[2])
      s += "</tr>\n"
    s += "</table>\n"
    return s

#-- colorPalette --
###################


def allPalettesToHTML():
  """
  Generate as simple html table showing all the different
  palette types available.
  """
  nc = 125
  pals = []
  cp = colorPalette(n=1)
  names = list(cp.keys())
  for name in names:
    pals.append(colorPalette(ptype=name,n=nc))
  s = ""
  s += "<table border='0'>\n"
  s += "<tr>"
  s += "<th>Palette</th>"
  for i in range(0,nc+1):
    s += "<th>%d</th>" % (i)
  s += "</tr>\n"
  for p in pals:
    s += "<tr>"
    s += "<td>%s</td>" % (p.ptype)
    for i in range (0,nc+1):
      c = p[i]
      s += "<td bgcolor='#%02X%02X%02X'>&nbsp;&nbsp;&nbsp;&nbsp;</td>" % (c[0],c[1],c[2])
    s += "</tr>\n"
  s += "</table>\n"
  return s

####################
#if __name__ == '__main__':
#  #Create a HTML page with all palettes
#  print "<html><head><title>Colors from colorPalette.py</title></head>\n"
#  print "<body>\n"
#  print """
#  <h1>colorpalette.py</h1>
#  <p>This page shows all the palette types available in colorPalette.  The safe palette is limited to 125 entries.
#  Other palettes can have from 1 to 256 entries, with the default being 256.  Each palette has a single alpha
#  (transparent) color that is not show here, but defaults to index 0 in all palettes.</p>
#  <h2>Palettes</h2>
#        """
#  print allPalettesToHTML()
#
#  #now output the documentation of this module.
#  print "<h2>pyDoc generated documentation for this module</h2>"
#  import pydoc
#  import colorpalette as cp
#  html = pydoc.HTMLDoc()
#  print html.document(cp)
#  print "</body>\b"
#  print "</html>"
