"""
@summary: This module tests the LmCommon.encoding.contrasts module
@author: CJ Grady
@version: 1.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import numpy as np
import os

from LmCommon.encoding.contrasts import BioGeoEncoding, PhyloEncoding

#TODO: This should not reference my local machine.  Integrate this into testing
#         framework
BASE_DATA_DIR = "/home/cjgrady/jeff/"
OUT_DIR = os.path.join(BASE_DATA_DIR, "output")

# .............................................................................
if __name__ == "__main__":
   
   gridDloc = os.path.join(BASE_DATA_DIR, "TenthDegree_Grid_FL-2462.shp")
   
   contrastsDloc = os.path.join(BASE_DATA_DIR, "goodContrasts/MergedContrasts_Florida.shp")

   #merged = BioGeoEncoding(contrastsDloc, gridDloc, eventField="event")
   merged = BioGeoEncoding(gridDloc)
   merged.addLayers(contrastsDloc, eventField="event")
   
   shpDir = os.path.join(BASE_DATA_DIR, "goodContrasts")
   shpList = ["ApalachicolaRiver.shp","GulfAtlantic.shp","Pliocene.shp"]
   pathList = []
   for shp in shpList:
      fn = os.path.join(shpDir, shp)
      pathList.append(fn)
   
   collection = BioGeoEncoding(gridDloc)
   collection.addLayers(pathList)
   
   #merged.buildContrasts()
   bg1 = merged.encodeHypotheses()
   
   testMerged = np.load(os.path.join(OUT_DIR, "test_mergedFA.npy"))
   
   #merged.writeBioGeoMtx(os.path.join(OUT_DIR, "mergedFA.npy"))
   
   
   #assert np.all(testMerged == merged.encMtx)
   assert np.all(abs(np.sum(testMerged, axis=0)) == abs(np.sum(bg1, axis=0)))
   
   
   #collection.buildContrasts()
   bg2 = collection.encodeHypotheses()
   #collection.writeBioGeoMtx(os.path.join(OUT_DIR, "collectionFA.npy"))
   testCollection = np.load(os.path.join(OUT_DIR, "test_collectionFA.npy"))

   assert np.all(abs(np.sum(testCollection, axis=0)) == abs(np.sum(bg2, axis=0)))
   
   #################  end BioGeo  ######################## 
   #
   #
   tree = {
      "name": "0",
      "path":  [0],
      "pathId": 0,
      "length": 0.0,
      "children": [
         {
            "pathId": 1,
            "length": .4,
            "path": [1,0],
            "children": [
               {
                  "pathId" : 2,
                  "length": .15,
                  "path": [9,5,0],
                  "children": [
                     {
                        "pathId" : 3,
                        "length" : .65,
                        "path": [3,2,1,0],
                        "children": [
                           {
                              "pathId" : 4,
                              "length" : .2,
                              "path" : [4,3,2,1,0],
                              "mx" : 0
                           },
                           {
                              "pathId" : 5,
                              "length" : .2,
                              "path" : [5,3,2,1,0],
                              "mx" : 1
                           }
                        ]
                     },
                     {
                        "pathId" : 6,
                        "length" : .85,
                        "path" : [6,2,1,0],
                        "mx" : 2
                     }
                  ]
               },
               {
                  "pathId" : 7,
                  "length" : 1.0,
                  "path" : [7,1,0],
                  "mx" : 3
               }
            ]
         },
         {
            "pathId" : 8,
            "length": .9,
            "path": [8,0],
            "children": [
               {
                  "pathId" : 9,
                  "length" : .5,
                  "path" : [9,8,0],
                  "mx" : 4
               },
               {
                  "pathId" : 10,
                  "length" : .5,
                  "path" : [10,8,0],
                  "mx" : 5
               }
            ]
         } 
      ]
   }
   
   I = np.random.choice(2,24).reshape(4,6)
   
   treeEncodeObj = PhyloEncoding(tree,I)
   
   p1 = treeEncodeObj.encodePhylogeny()

   testP = np.array([
      [-0.1956521739130435, -0.2804878048780488, -0.5, -1.0, 0.0], 
      [-0.1956521739130435, -0.2804878048780488, -0.5, 1.0, 0.0], 
      [-0.2898550724637681, -0.4390243902439025, 1.0, 0.0, 0.0], 
      [-0.31884057971014496, 1.0, 0.0, 0.0, 0.0], 
      [0.5, 0.0, 0.0, 0.0, -1.0], 
      [0.5, 0.0, 0.0, 0.0, 1.0]])
   
   print p1
   
   assert round(np.sum(p1), 3) == 0.000
   

   tree2 = {
      "name": "0",
      "pathId": 0,
      "children": [
         {
            "pathId": 1,
            "children": [
               {
                  "pathId" : 2,
                  "children": [
                     {
                        "pathId" : 3,
                        "children": [
                           {
                              "pathId" : 4,
                              "mx" : 0
                           },
                           {
                              "pathId" : 5,
                              "mx" : 1
                           }
                        ]
                     },
                     {
                        "pathId" : 6,
                        "mx" : 2
                     }
                  ]
               },
               {
                  "pathId" : 7,
                  "mx" : 3
               }
            ]
         },
         {
            "pathId" : 8,
            "children": [
               {
                  "pathId" : 9,
                  "mx" : 4
               },
               {
                  "pathId" : 10,
                  "mx" : 5
               }
            ]
         } 
      ]
   }
   
   i2 = np.random.choice(2,24).reshape(4,6)
   
   treeEncodeObj2 = PhyloEncoding(tree2, i2)
   
   p2 = treeEncodeObj2.encodePhylogeny()

   print p2
   assert np.sum(p2) == 0.0

   
   
   #### Test BioGeo ####
  
   # Tashi
   #Contrastsdloc = '/home/jcavner/TASHI_PAM/Test/PAIC.shp'
   #EventField = "PAIC"
   # GridDloc = '/home/jcavner/TASHI_PAM/Test/Grid_5km.shp'
   #########################
   # Charolette
   # Merged
   # intersect grid
   #GridDloc = "/home/jcavner/BiogeographyMtx_Inputs/Florida/TenthDegree_Grid_FL-2462.shp"
   #
   #Contrastsdloc ="/home/jcavner/BiogeographyMtx_Inputs/Florida/GoodContrasts/MergedContrasts_Florida.shp"
   ##contrastsdLoc, intersectionLyrDLoc, EventField=False
   #merged = BioGeo(Contrastsdloc,GridDloc,EventField="event")
   # 
   #base = "/home/jcavner/BiogeographyMtx_Inputs/Florida/GoodContrasts"
   #shpList = ["ApalachicolaRiver.shp","GulfAtlantic.shp","Pliocene.shp"]
   #pathList = []
   #for shp in shpList:
   #   fn = os.path.join(base,shp)
   #   pathList.append(fn)
   #
   #collection = BioGeo(pathList,GridDloc)
   #
   #merged.buildContrasts()
   #merged.writeBioGeoMtx("/home/jcavner/testBioGeoEncoding/mergedFA.npy")
   # 
   #collection.buildContrasts()
   #collection.writeBioGeoMtx("/home/jcavner/testBioGeoEncoding/collectionFA.npy")
   
   
   #################  end BioGeo  ######################## 
   #
   #
   #tree = {"name": "0",
   #     "path": "0",
   #     "pathId": "0",
   #     "children":[
   #                 {"pathId":"1","length":".4","path":"1,0",
   #                 "children":[
   #                             {"pathId":"2","length":".15","path":"9,5,0",
   #                              "children":[
   #                                          {"pathId":"3","length":".65","path":"3,2,1,0",
   #                                           
   #                                           "children":[
   #                                                       {"pathId":"4","length":".2","path":"4,3,2,1,0","mx":"0"},
   #                                                       {"pathId":"5","length":".2","path":"5,3,2,1,0","mx":"1"}
   #                                                       ]
   #                                           
   #                                           },
   #                                          
   #                                          {"pathId":"6","length":".85","path":"6,2,1,0","mx":"2"}
   #                                          
   #                                          ]
   #                              
   #                              },
   #                              {"pathId":"7","length":"1.0","path":"7,1,0","mx":"3"}
   #                             
   #                             ] },
   #                 
   #
   #                 {"pathId":"8","length":".9","path":"8,0",
   #                  "children":[{"pathId":"9","length":".5","path":"9,8,0","mx":"4"},{"pathId":"10","length":".5","path":"10,8,0","mx":"5"}] } 
   #                 ]
   #     
   #     }
   #
   #I = np.random.choice(2,24).reshape(4,6)
   #
   #treeEncodeObj = PhyloEncoding(tree,I)
   #
   #P, I, internal = treeEncodeObj.makeP(True)
   #print P
   
