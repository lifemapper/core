#from Pedro_Analysis.MCPA.encoding.lmTree import LMtree as LT
from lmTree import LMtree as LT
#from Pedro_Analysis.MCPA.encoding.contrasts import PhyloEncoding, BioGeo
from contrasts import PhyloEncoding, BioGeo
import os
import numpy as np
import argparse
from __builtin__ import None


def TestTreeModule(inputFn, outputFn=None, tipsToDrop=None):
   """
   @param inputFn: file path to either json or newick
   @note: what happens if mtxIdx (mx) in tree?
   """
   tmo = LT.fromFile(inputFn)
   
   if not tmo.binary:
      resolvedTree = tmo.resolvePoly()  # returns new PhyloEncoding object
   else:
      resolvedTree = tmo   
   if tipsToDrop is not None:
      finalTree = resolvedTree.dropTips(tipsToDrop)  # returns new PhyloEncoding object
   else:
      finalTree = resolvedTree
   if outputFn is not None:
      finalTree.writeTree(outputFn)
      
   return finalTree

def TestTreeEncoding(treeFn, pamFn, ultra, treeDict = None, pam = None, outFn=None):
   """
   @param treeFn: file path to tree with mtxIdx ('mx') in tips
   @param pamFn: file path to pam .npy
   """
   if treeDict is not None and pam is not None:
      emo = PhyloEncoding(treeDict,pam)
   else:
      emo = PhyloEncoding.fromFile(treeFn, pamFn)
      
   # encode
   if ultra:
      brLen = True
   else:
      brLen = False
   P,I,internal = emo.makeP(brLen)
   if outFn is not None:
      np.save(outFn,P)   
      
def TestBioGeoEncoding(intersectLyrFn, contrasShpList, EventField=False, writeBioEncFn=None):
   """
   @param EventField: optional, requried only for non-collections (merged or mut. exclus.)
   """
   biogeo = BioGeo(contrasShpList, intersectLyrFn, EventField = EventField)
   biogeo.buildContrasts()
   if writeBioEncFn is not None:
      biogeo.writeBioGeoMtx(writeBioEncFn)

# .............................................................................
if __name__ == "__main__":
   
   parser = argparse.ArgumentParser(description="tests tree module and encoding both phylo and biogeo")
   
   # tree inputs
   parser.add_argument("incidenceMtxFn")
   parser.add_argument("treeJSON_MX_Fn")
   parser.add_argument("tree_NHX_tre_Fn")  # RAxML_bestTree.12.15.14.1548tax.ultrametric.tre or RAxML_bestTreeRootedphylogram.nhx
   parser.add_argument('-td', dest='tipsToDrop')
   parser.add_argument('-two', dest='modifedTreeWriteFn')
   parser.add_argument('-teo', dest='encTreeMtxWriteFn')
   
   #biogeo inputs
   parser.add_argument("intersectLyrFn")
   parser.add_argument("contrastShpList")
   parser.add_argument('-ef', dest='eventField')
   parser.add_argument('-beo', dest='bioEncOutFn')
   
   args = parser.parse_args()
   
   ## tree prep test
   inputFn = args.tree_NHX_tre_Fn
   if args.tipsToDrop is None:
      td = ["Eugenia_axillaris","Chamaecrista_fasciculata"]
   else:
      td = args.tipsToDrop
   
   tree = TestTreeModule(inputFn, tipsToDrop = td,
                         outputFn=args.modifedTreeWriteFn)
   
   ## tree encoding test
   inputFn = args.treeJSON_MX_Fn
   pamFn = args.incidenceMtxFn
   tmo = LT.fromFile(inputFn)
   ultra = tmo.checkUltraMetric()
   TestTreeEncoding(inputFn, pamFn, ultra, outFn=args.encTreeMtxWriteFn)
   
   #############
   
   #BioGeo Encoding Test
   if args.eventField is None:
      event = "event"
   else:
      event = args.eventField
   TestBioGeoEncoding(args.intersectLyrFn, args.contrastShpList, 
                      EventField = args.eventField, writeBioEncFn=args.bioEncOutFn)
   
   
