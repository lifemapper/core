#!/bin/bash
"""
@summary: This script updates a Lifemapper object in the database
@version: 4.0.0
@status: beta

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
import argparse
import json
import glob
import os
import shutil

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import ProcessType, LMFormat, JobStatus
from LmServer.base.layer2 import Vector, Raster
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class Stockpile(LMObject):
    """
    @summary: Object to receive results of MF commands, update database with 
                 success status.
    """
# .............................................................................
    @classmethod
    def testAndStash(cls, ptype, objId, status, successFname, outputFnameList,
                          metaFilename=None):
        """
        @summary: Test output files and update DB with status.  Only success for
                     all outputs = JobStatus.COMPLETE 
        @param ptype: LmCommon.common.lmconstants.ProcessType for the process
                 being examined
        @param objId: Unique database ID for the object to update
        @param status: Not currently used, currently assign COMPLETE/ERROR from
                 success/failure of testFile function. TODO: Could be status 
                 returned by the computational process??
        @param successFname: Filename to be written IFF success=True. Contains  
                 final status and testing results 
        @param outputFnameList: List of output files returned by the computational 
                 process
        @param metaFilename: filename for JSON matrix metadata 
        @return: True on success, False on bad data or failure to get/copy/update
        @todo: Use or delete status parameters
        """
        outputInfo = []
        success = True
        
        # Check incoming status
        if status >= JobStatus.GENERAL_ERROR:
            success = False
            outputInfo.append('Incoming status value was {}'.format(success))
        else:
            # Test each file
            for fname in outputFnameList:
                currSuccess, msgs = cls.testFile(fname)
                if not currSuccess:
                    success = False
                    outputInfo.extend(msgs)
                    
            if not success:
                status = JobStatus.GENERAL_ERROR
            else:
                status = JobStatus.COMPLETE

        # Update database
        scribe = BorgScribe(ConsoleLogger())
        scribe.openConnections()
        try:
            obj = cls._getObject(scribe, ptype, objId)
            cls._copyObject(scribe, ptype, obj, outputFnameList, metaFilename)
            cls._updateObject(scribe, obj, status)
        except Exception, e:
            print('Exception on Stockpile._updateObject ({})'.format(str(e)))
            success = False
            raise
        finally:
            scribe.closeConnections()
            
        return success
        
# .............................................................................
    @classmethod
    def _getObject(cls, scribe, ptype, objId):
        """
        @summary: Get object and update DB with status.  
        """
        # Get object
        obj = None
        try:
            if ProcessType.isOccurrence(ptype):
                obj = scribe.getOccurrenceSet(occId=objId)
            elif ProcessType.isProject(ptype):
                obj = scribe.getSDMProject(objId)
            elif ptype == ProcessType.RAD_BUILDGRID:
                obj = scribe.getShapeGrid(lyrId=objId)
            elif ProcessType.isMatrix(ptype):
                obj = scribe.getMatrix(mtxId=objId)
            elif ProcessType.isIntersect(ptype):
                obj = scribe.getMatrixColumn(mtxcolId=objId)
            else:
                raise LMError(currargs='Unsupported ProcessType {} for object {}'
                                  .format(ptype, objId))
        except Exception, e:
            raise LMError(currargs='Failed to get object {} for process {}, exception {}'
                              .format(objId, ptype, str(e)))
        if obj is None:
            raise LMError(currargs='Failed to get object {} for process {}'
                              .format(objId, ptype))
        return obj

# .............................................................................
    @classmethod
    def _copyObject(cls, scribe, ptype, obj, fileNames, metaFilename):
        """
        @summary: Get object and update DB with status.  
        """
        metadata = None
        try:
            with open(metaFilename) as inMeta:
                metadata = json.load(inMeta)
        except:
            pass
        # Copy data
        try:
            if ProcessType.isOccurrence(ptype) and os.path.getsize(fileNames[0]) > 0:
                # Move data file
                baseOutDir = os.path.dirname(obj.getDLocation())
                for fn in glob.glob('{}.*'.format(os.path.splitext(fileNames[0])[0])):
                    shutil.copy(fn, baseOutDir)
                # Try big data file
                bigFname = fileNames[0].replace('/pt', '/bigpt')
                if cls.testFile(bigFname)[0]:
                    shutil.copy(bigFname, obj.getDlocation(largeFile=True))            
            elif ProcessType.isProject(ptype) and os.path.getsize(fileNames[0]) > 0:
                shutil.copy(fileNames[0], obj.getDLocation())
                shutil.copy(fileNames[1], obj.getProjPackageFilename())
            elif ProcessType.isMatrix(ptype) and os.path.getsize(fileNames[0]) > 0:
                if metadata is not None:
                    obj.addMtxMetadata(metadata)
                shutil.copy(fileNames[0], obj.getDLocation())
        except Exception, e:
            raise LMError(currargs='Exception copying primary {} or ancillary output, ({})'
                              .format(obj.getDLocation(), str(e)))

# .............................................................................
    @classmethod
    def _updateObject(cls, scribe, obj, status):
        """
        @summary: Get object and update DB with status.  
        """
        # Update verify hash and modtime for layers
        try:
            obj.updateLayer()
        except:
            pass

        obj.updateStatus(status)

        # Update database
        try:
            scribe.updateObject(obj)
        except Exception, e:
            raise LMError(currargs='Exception updating object {} for process {}, ({})'
                              .format(objId, ptype, str(e)))

    # ...............................................
    @classmethod
    def testFile(cls, outputFname):
        success = True
        msgs = []
        _, ext = os.path.splitext(outputFname)
        if not os.path.exists(outputFname):
            msgs.append('File {} does not exist'.format(outputFname))
            success = False
        elif LMFormat.isTestable(ext):
            if LMFormat.isGeo(ext):
                fileFormat = LMFormat.getFormatByExtension(ext)
                if LMFormat.isOGR(ext=ext):
                    success, featCount = Vector.testVector(outputFname, 
                                                           driver=fileFormat.driver)
                    if not success:
                        try:
                            f = open(outputFname, 'r')
                            msg = f.read()
                            f.close()
                            msgs.append(msg)
                        except:
                            pass
                        msgs.append('File {} is not a valid {} file'.format(outputFname, 
                                                                                     fileFormat.driver))
                    elif featCount < 1:
                        msgs.append('Vector {} has no features'.format(outputFname))
                        
                elif LMFormat.isGDAL(ext=ext):
                    success = Raster.testRaster(outputFname)
                    if not success:
                        msgs.append('File {} is not a valid GDAL file'.format(outputFname))
            else:
                f = open(outputFname, 'r')
                data = f.read()
                f.close()
                if LMFormat.isJSON(ext):
                    import json
                    try:
                        json.loads(data)
                    except:
                        success = False
                        msgs.append('File {} does not contain valid JSON'
                                        .format(outputFname))
        return success, msgs

# .............................................................................
if __name__ == "__main__":
    """
    @todo: Use or delete status parameters
    """
    parser = argparse.ArgumentParser(description='This script updates a Lifemapper object')
    # Inputs
    parser.add_argument('processType', type=int, 
                              help='The process type of the object to update')
    parser.add_argument('objectId', type=int, 
                              help='The id of the object to update')
    parser.add_argument('successFilename', type=str, 
                              help=('File to be created only if the job was ' + 
                                      'completed successfully.'))
    parser.add_argument('objectOutput', type=str, nargs='*', 
                              help=('Files to sanity check. '))
    # Status arguments
    parser.add_argument('-s', dest='status', type=int, 
                              help='The status to update the object with')
    parser.add_argument('-f', dest='status_file', type=str, 
                              help='A file containing the new object status')
    
    # Metadata filename
    parser.add_argument('-m', dest='metadataFilename', type=str, 
                             help='A JSON file containing metadata about this object')
    args = parser.parse_args()
    
    # Status comes in as an integer or file 
    status = None
    if args.status is not None:
        status = args.status
    elif args.status_file is not None:
        try:
            with open(args.status_file) as statusIn:
                status = int(statusIn.read())
        except:
            # Need to catch empty status file
            status = JobStatus.GENERAL_ERROR
    
    if args.metadataFilename is not None:
        metaFilename = args.metadataFilename
    else:
        metaFilename = None
        
    ptype = args.processType
    objId = args.objectId
    successFname = args.successFilename
    outputFnameList = args.objectOutput
    
    success = Stockpile.testAndStash(ptype, objId, status, successFname, 
                                     outputFnameList, metaFilename=metaFilename)
        
    if success:
        # Only write success file if successfully updated an object with 
        #     non-error status.  Otherwise, Makeflow should stop and that will 
        #     happen without this file
        with open(successFname, 'w') as successOut:
            successOut.write('1')

    
"""
Call like:
$PYTHON stockpile.py <ptype>  <objId>  <successFname>  <outputFnameList>  
                    <-s status OR -f statusFname>

$PYTHON /opt/lifemapper/LmServer/tools/stockpile.py 420 504 pt_504.success /share/lm/data/archive/ryan/000/000/000/504/pt_504.shp


import os
from LmCommon.common.lmconstants import ProcessType, LMFormat, JobStatus
from LmServer.base.layer2 import Vector, Raster
from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.tools.stockpile import *

scribe = BorgScribe(ConsoleLogger())
scribe.openConnections()



status = 999
ptype = ProcessType.USER_TAXA_OCCURRENCE
objId = 504
successFname = 'pt_504.success'
outputFnameList = ['/share/lm/data/archive/ryan/000/000/000/504/pt_504.shp']

Stockpile.testAndStash(ptype, objId, status, successFname, outputFnameList)

scribe.closeConnections()
"""
    
