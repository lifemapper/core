import numpy as np
import json
import os


def getCharoletteTest():
   
   #base = "/home/jcavner/BiogeographyMtx_Inputs/Florida/"
   #
   #dLocBioGeoEvents = "/home/jcavner/BiogeographyMtx_Inputs/Florida/GoodContrasts/MergedContrasts_Florida.shp"
   #PAMdLoc = os.path.join(base,"fullpam_float_2.npy")  # THIS NEEDS TO BE FLOAT !!!!
   #dLocTreeJSON = os.path.join(base,'tree_2_exp1800.json') # NEED TO USE NEW TREE !!!
   #dLocShpGrid = os.path.join(base,"Florida_6_Minutes-2467.shp")  
   #EventField = "Event"
   #
   #I,P,B,eventPos,internal = constructContrasts(PAMdLoc, dLocTreeJSON, dLocBioGeoEvents, dLocShpGrid,
   #                                    EventField)
   #cPickle.dump(internal,open(os.path.join("/home/jcavner/BiogeographyMtx_Inputs/Florida/outputs","internal_2.pkl"),"wb"))
   #
   #NodeMtx = P  # setting global
   #print "calcing observed env"
   ########   Observed Environment #######
   #E = np.load(os.path.join(base,"grim.npy"))
   pass

# .............................................
def makeInputsForTextTest():
   
   # incidence matrix fromt the text
   I = np.array([[1, 0, 0, 1, 0, 0],
                 [0, 0, 1, 1, 0, 0],
                 [1, 0, 0, 1, 0, 1],
                 [0, 0, 1, 1, 0, 1],
                 [0, 1, 0, 1, 0, 1],
                 [0, 0, 0, 0, 1, 0],
                 [1, 0, 0, 0, 1, 0],
                 [0, 1, 0, 0, 1, 0]])
   
   # P from the text
   P = np.array([[-1.   , -0.5  , -0.25 , -0.125,  0.   ],
                 [ 1.   , -0.5  , -0.25 , -0.125,  0.   ],
                 [ 0.   ,  1.   , -0.5  , -0.25 ,  0.   ],
                 [ 0.   ,  0.   ,  1.   , -0.5  ,  0.   ],
                 [ 0.   ,  0.   ,  0.   ,  0.5  , -1.   ],
                 [ 0.   ,  0.   ,  0.   ,  0.5  ,  1.   ]])
   
   return P,I,
# .............................................
def getEnvTextMatrix():

   E = np.array([[1.3,  13.0, 100.0], 
                 [.78,  12.4, 121.0], 
                 [.85,  1.2,  99.0], 
                 [1.0,  0.98, 11.2], 
                 [4.8,  0.45,  21.23], 
                 [3.89, 0.99,  21.11], 
                 [3.97, 1.2,  12.01], 
                 [3.23, 1.0,  10.12] ])
   return E

def loadJSON(path):
   with open(path,'r') as f:
      jsonstr = f.read()
   return json.loads(jsonstr)

def getAllTestInputs(testWithInputsFromPaper=False):
   """
   @param shiftedTree: this means using a tree with same topology
   of Liebold example but with tips shifted around according to 
   how mx's are likely to appear in a real tree
   """
   pass