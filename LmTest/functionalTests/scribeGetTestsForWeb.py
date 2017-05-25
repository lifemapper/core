"""
@summary: Temporary script for testing scribe get functions
@note: Will be deprecated and incorporated into fuller testing suite
"""
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
if __name__ == '__main__':
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   # Environmental layer
   envLyrList = scribe.listEnvLayers(0, 10)
   envLyr = scribe.getEnvLayer(envlyrId=envLyrList[0].id)
   
   # Layer
   lyrList = scribe.listLayers(0, 10)
   lyr = scribe.getLayer(lyrId=lyrList[0].id)
   
   # Occurrence layer
   occList = scribe.listOccurrenceSets(0, 10)
   occ = scribe.getOccurrenceSet(occId=occList[0].id)
   
   # projection
   prjList = scribe.listSDMProjects(0, 10)
   prj = scribe.getSDMProject(prjList[0].id)
   
   # Scenario
   scnList = scribe.listScenarios(0, 10)
   scn = scribe.getScenario(scnList[0].id)
   
   # ShapeGrid
   sgList = scribe.listShapegrids(0, 10)
   sg = scribe.getShapeGrid(lyrId=sgList[0].id)
   
   