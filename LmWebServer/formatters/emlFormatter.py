"""
@summary: Module functions for converting object to EML
@author: CJ Grady
@version: 2.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import cherrypy

from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmServer.legion.gridset import Gridset
from LmCommon.common.lmXml import Element, SubElement, tostring

# .............................................................................
def _createDataTable(dtObj):
   """
   @summary: Create data table subsection for an object
   """
   dtEl = Element('otherEntity', attrib={'id' : dtObj.name})
   SubElement(dtEl, 'entityName', value=dtObj.name)
   phys = SubElement(dtEl, 'physical')
   SubElement(phys, 'objectName', value='mtx_{}.csv'.format(dtObj.getId()))
   SubElement(phys, 'encodingMethod', value='ASCII')
   SubElement(
      SubElement(
         SubElement(phys, 'dataFormat'), 
         'externallyDefinedFormat'),
      'formatName', 
      value='Lifemapper Matrix Json')
   
   alEl = SubElement(dtEl, 'attributeList')
   for colHeader in dtObj.getColumnHeaders():
      SubElement(alEl, 'attribute', value=colHeader)
   return dtEl

# .............................................................................
def _createOtherEntity(entityObj):
   """
   @summary: Create other entity subsection for an object
   """
   oeEl = Element('otherEntity', attrib={'id' : entityObj.name})
   SubElement(oeEl, 'entityName', value=entityObj.name)
   phys = SubElement(oeEl, 'physical')
   SubElement(phys, 'objectName', value='tree_{}.nex'.format(entityObj.getId()))
   SubElement(phys, 'encodingMethod', value='ASCII')
   SubElement(
      SubElement(
         SubElement(phys, 'dataFormat'), 
         'externallyDefinedFormat'),
      'formatName', 
      value='nexus')
   SubElement(oeEl, 'entityType', value='tree')
   return oeEl

# .............................................................................
def _createSpatialVector(svObj):
   """
   @summary: Create spatial vector subsection for an object
   """
   svId = 'mtx_{}'.svObj.getId()
   svEl = Element('spatialVector', attrib={'id' : svId})
   phys = SubElement(svEl, 'physical')
   SubElement(phys, 'objectName', value='mtx_{}.geojson'.format(svObj.getId()))
   SubElement(phys, 'encodingMethod', value='ASCII')
   SubElement(
      SubElement(
         SubElement(phys, 'dataFormat'), 
         'externallyDefinedFormat'),
      'formatName', 
      value='geojson')
   
   alEl = SubElement(svEl, 'attributeList')
   for colHeader in svObj.getColumnHeaders():
      SubElement(alEl, 'attribute', value=colHeader)
      
   SubElement(svEl, 'geometry', value='polygon')
   
   return svEl

# .............................................................................
def makeEml(myObj):
   """
   @summary: Generate an EML document representing metadata for the provided 
                object
   """
   # TODO: Add name
   if isinstance(myObj, Gridset):
      topEl = Element('eml', 
                      attrib={
                         # TODO: Better package ids
                        'packageId' : 'org.lifemapper.gridset.{}'.format(
                           myObj.getId()),
                        'system' : 'http://svc.lifemapper.org'
                       })
      dsEl = SubElement(topEl, 'dataset', 
                        attrib={'id' : 'gridset_{}'.format(myObj.getId())})
      # Contact
      SubElement(SubElement(dsEl, 'contact'), 'organizationName', value='Lifemapper')
      
      for mtx in myObj.getMatrices():
         # TODO: Enable GRIMs
         if mtx.matrixType in [MatrixType.ANC_PAM, #MatrixType.GRIM, 
                               MatrixType.PAM, MatrixType.SITES_OBSERVED]:
            dsEl.append(_createSpatialVector(mtx))
         elif mtx.matrixType in [MatrixType.ANC_STATE, 
                                 MatrixType.DIVERSITY_OBSERVED, 
                                 MatrixType.MCPA_OUTPUTS,
                                 MatrixType.SPECIES_OBSERVED]:
            dsEl.append(_createDataTable(mtx))
      if myObj.tree is not None:
         dsEl.append(_createOtherEntity(myObj.tree))
   else:
      raise Exception, 'Cannot create eml for {} currently'.format(myObj.__class__)
   return topEl
   
# .............................................................................
def emlObjectFormatter(obj):
   """
   @summary: Looks at object and converts to EML based on its type
   """
   response = _formatObject(obj)
   
   return tostring(response)

# .............................................................................
def _formatObject(obj):
   """
   @summary: Helper method to format an individual object based on its type
   """
   cherrypy.response.headers['Content-Type'] = LMFormat.EML.getMimeType()
   
   if isinstance(obj, Gridset):
      cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="{}.eml"'.format(obj.name)
      return makeEml(obj)
   else:
      raise TypeError, "Cannot format object of type: {}".format(type(obj))

