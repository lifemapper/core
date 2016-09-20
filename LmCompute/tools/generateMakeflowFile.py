"""
@summary: This script builds a makeflow request file
@author: CJ Grady
@note: This first version is very simple.  It takes two arguments, the output
          file name and a file with projection job ids and their dependent 
          model ids.  This should be expanded and made more robust in 
          subsequent versions
"""
import sys
from LmCompute.common.lmconstants import WORKSPACE_PATH
from LmCompute.common.localconstants import (JOB_REQUEST_PATH, PYTHON_CMD)

# NOTE: Fill these in from constants

JOB_RUNNER_FACTORY = ""
REQUEST_FILL_SCRIPT = ""

SETUP_STRING = """\
# Example experiment makeflow

JOB_REQUESTS_DIR={reqDir}

PYTHON={python}
RUNNER={runnerFactory}
REQ_FILL={reqFill}
OUT_DIR={outDir}/completed
""".format(reqDir=JOB_REQUEST_PATH, python=PYTHON_CMD, runnerFactory=JOB_RUNNER_FACTORY,
           reqFill=REQUEST_FILL_SCRIPT, outDir=WORKSPACE_PATH)

# .............................................................................
def getProjectionsFromFile(prjFn):
   prjs = []
   with open(prjFn) as inF:
      for line in inF:
         prj, mdl = line.split(",")
         prjs.append((prj.strip(), mdl.strip()))
   return prjs

# .............................................................................
if __name__ == "__main__":
   if len(sys.argv) == 3:
      outFn = sys.argv[1]
      prjFn = sys.argv[2]
      prjs = getProjectionsFromFile(prjFn)
      mdls = set([])
      
      with open(outFn, 'w') as outF:
         outF.write(SETUP_STRING)
         
         for prj, mdl in prjs:
            prjJobs = """\
# Projection {prjJobId}
$OUT_DIR/120-{prjJobId}.tif.tar.gz : $OUT_DIR/120-{prjJobId}.tif
   tar -czf $OUT_DIR/120-{prjJobId}.tif.tar.gz $OUT_DIR/120-{prjJobId}.tif

$OUT_DIR/120-{prjJobId}.tif $OUT_DIR/120-{prjJobId}.zip: $OUT_DIR/120-{prjJobId}Req.xml
   $PYTHON $RUNNER $OUT_DIR/120-{prjJobId}Req.xml

$OUT_DIR/120-{prjJobId}Req.xml: $JOB_REQUESTS_DIR/120-{prjJobId}Req.xml.part $OUT_DIR/110-{mdlJobId}.txt
   $PYTHON $REQ_FILL $JOB_REQUESTS_DIR/120-{prjJobId}Req.xml.part $OUT_DIR/110-{mdlJobId}.txt $OUT_DIR/120-{prjJobId}Req.xml
""".format(prjJobId=prj, mdlJobId=mdl)
            outF.write(prjJobs)
            mdls.add(mdl)
         
         for mdl in mdls:
            mdlJob = """\
$OUT_DIR/110-{mdlJobId}.txt $OUT_DIR/110-{mdlJobId}.zip: $JOB_REQUESTS_DIR/110-{mdlJobId}Req.xml
   $PYTHON $RUNNER $JOB_REQUESTS_DIR/110-{mdlJobId}Req.xml
""".format(mdlJobId=mdl)
            outF.write(mdlJob)
   else:
      print "Usage: generateMakeflowFile.py [output makeflow file name] [projection jobs]"
      
      



