"""
@summary Module containing phylogenetic statistics from Stephen Smith
@author Stephen Smith / CJ Grady
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
import numpy as np

import LmCompute.plugins.multi.calculate.tree_utils as tree_utils

# .............................................................................
def mnnd(tree, tips):
   """
   @summary: Mean Nearest Neighbor Distance
   @param tree: Local tree object from Newick @see tree_reader and tree_utils
   @param tips: The tips present for any given site
   @todo: This could probably be parallelized to be done for all sites at once
             using numpy
   @note: Taken from the mnnd module in Stephen's biotaphy_scripts repository 
   """
   distsd = {} # tip and list of distances
   lt = list(tips)
   for i in lt:
      distsd[i] = []
   for i in range(len(lt)):
      for j in range(len(lt)):
         if i < j:
            m = phylo_dist(lt[i],lt[j],tree)
            distsd[lt[i]].append(m)
            distsd[lt[j]].append(m)
   dists = []
   for i in distsd:
      dists.append(min(distsd[i]))
   measure = np.mean(dists)
   return measure

# .............................................................................
def mpd(tree, tips):
   """
   @summary: Mean Phylogenetic Distance between all pairs of taxa for a site
   @param tree: Local tree object from Newick @see tree_reader and tree_utils
   @param tips: The tips present for any given site
   @todo: This could probably be parallelized to be done for all sites at once
             using numpy
   @note: Taken from the mpd module in Stephen's biotaphy_scripts repository 
   """
   dists = []
   lt = list(tips)
   for i in range(len(lt)):
      for j in range(len(lt)):
         if i < j:
            dists.append(phylo_dist(lt[i],lt[j],tree))
   measure = np.mean(dists)
   return measure

# .............................................................................
def pd(tree, tips):
   """
   @summary: Phylogenetic diversity - Sum of the branch lengths for the minimum
                spanning path of the involved taxa for a site
   @param tree: Local tree object from Newick @see tree_reader and tree_utils
   @param tips: The tips present for any given site
   @todo: This could probably be parallelized to be done for all sites at once
             using numpy
   @note: Taken from the pd module in Stephen's biotaphy_scripts repository 
   """
   traveled = set()
   traveled.add(tree_utils.get_mrca(list(tips), tree))
   measure = 0
   for i in tips:
      c = i
      while c not in traveled:
         measure += c.length
         if c.parent == None: #root
            break
         traveled.add(c)
         c = c.parent
   return measure

# .............................................................................
def phylo_dist(tip1, tip2, tree):
   """
   @summary: Get the phylogenetic distance between two tips
   @note: Taken from the mpd module in Stephen's biotaphy_scripts repository 
   @todo: This could probably go into the tree module or if we switch tree 
             code to ours, there is probably a function in dendropy
   """
   mrca = tree_utils.get_mrca([tip1,tip2],tree)
   d1 = 0
   d2 = 0
   cn = tip1
   while cn != mrca:
      d1 += cn.length
      cn = cn.parent
   cn = tip2
   while cn != mrca:
      d2 += cn.length
      cn = cn.parent
   return d1+d2

# .............................................................................
def spd(tree, tips):
   """
   @summary: Sum of Phylogenetic Distances for the taxa present at a site
   @param tree: Local tree object from Newick @see tree_reader and tree_utils
   @param tips: The tips present for any given site
   @todo: This could probably be parallelized to be done for all sites at once
             using numpy
   @note: Taken from the spd module in Stephen's biotaphy_scripts repository 
   """
   dists = []
   ncompared = 0
   su = 0
   lt = list(tips)
   for i in range(len(lt)):
      for j in range(len(lt)):
         if i < j:
            m = phylo_dist(lt[i],lt[j],tree)
            dists.append(m)
            ncompared += 1
   measure = ncompared * np.mean(dists)
   return measure
