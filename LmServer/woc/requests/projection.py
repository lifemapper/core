class ProjectionWoC(object):
   def __init__(self, prjScns, mdlScn, algos, occSets):
      if not isinstance(occSets, list):
         self.occSets = [occSets]
      else:
         self.occSets = occSets
         
      if not isinstance(algos, list):
         self.algos = [algos]
      else:
         self.algos = algos
      
      self.mdlScn = mdlScn
      
      if not isinstance(prjScns, list):
         self.prjScns = [prjScns]
      else:
         self.prjScns = prjScns
   
   def getItems(self):
      projections = []
      for occ in self.occSets:
         for algo in self.algos:
            for prjScn in self.prjScns:
               # return projection
               projections.append(newPrj)
      return projections