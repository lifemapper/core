#from Pedro_Analysis.MCPA.encoding.lmTree import LMtree as LT
from lmTree import LMtree as LT
#from Pedro_Analysis.MCPA.encoding.contrasts import PhyloEncoding, BioGeo
from contrasts import PhyloEncoding, BioGeo
import os
import numpy as np


base = "/home/jcavner/Charolettes_Data/Trees"
ultrametric = "UltraMetricCopyWithNHXExt.nhx"

ultraPth = os.path.join(base,ultrametric)


tmo = LT.fromFile(ultraPth)

listDir = os.listdir("/home/jcavner/Charolettes_Data/Thresholed_GeoTiff")

lyrnames = [x.replace(".tif","") for x in listDir if ".xml" not in x]

print len(lyrnames)

print tmo.tipCount

tipsToDrop = [x for x in tmo.tipNames if x not in lyrnames]


resolvedTree = tmo.resolvePoly()
prunedResolved = resolvedTree.dropTips(tipsToDrop)

#prunedResolved.writeTree("/home/jcavner/BBBB.json")

PAMDlocBase = "/home/jcavner/ghWorkspace/analysis.git/Pedro_Analysis/MCPA/testData/CharolettesData"
PAMName = "fullpam_float_2.npy"
#
pamPath = os.path.join(PAMDlocBase,PAMName)
#
#print prunedResolved.binary
#print prunedResolved.polytomies
#print prunedResolved.tipCount
#

resolvePrunedWithMX = "/home/jcavner/DermotWS_NOW/addMx_1835/tree/tree.json"

emo = PhyloEncoding.fromFile(resolvePrunedWithMX,pamPath)
print "encoding"
P,I,internal = emo.makeP(True)
#
np.save("/home/jcavner/TestCharoletteEncBrLen_compare.npy",P)

print "finished"
#print P.shape
