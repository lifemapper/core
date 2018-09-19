"""
@summary Module containing phylogenetic statistics from Stephen Smith 
            (rewritten by CJ)
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

# .............................................................................
def mean_nearest_taxon_distance(pam, phylo_dist_mtx):
    """Calculates the nearest neighbor distance for each site in a PAM
    """
    mntd = []
    for site in pam.data:
        sp_idxs = np.where(site == 1)[0]
        
        if len(sp_idxs) > 1:
            nearest_total = 0.0
            for i in sp_idxs:
                nearest = 99999
                for j in sp_idxs:
                    cmp_val = phylo_dist_mtx.data[i, j]
                    if cmp_val > 0.0 and cmp_val < nearest:
                        nearest = cmp_val
                nearest_total += nearest
            mntd.append(float(nearest_total) / len(sp_idxs))
        else:
            mntd.append(0.0)
    return np.array(mntd).reshape((len(mntd), 1))

# .............................................................................
def mean_pairwise_distance(pam, phylo_dist_mtx):
    """Calculates mean pairwise distance
    
    Calculates mean pairwise distance between the species present at each site
    """
    num_sites = pam.data.shape[0]
    mpd = np.zeros((num_sites, 1), dtype=float)
    for x in range(num_sites):
        sp_idxs = np.where(pam.data[x] == 1)[0]
        num_sp = len(sp_idxs)
        if num_sp > 1:
            num_pairs = num_sp * (num_sp - 1) / 2
            total_distance = 0.0

            for i in range(num_sp - 1):
                for j in range(i + 1, num_sp):
                    total_distance += phylo_dist_mtx.data[i, j]

            mpd[x, 0] = total_distance / num_pairs

    return mpd

# .............................................................................
def sum_pairwise_distance(pam, phylo_dist_mtx):
    """Calculates the sum pairwise distance for all species present at a site
    
    TODO:
        Consider combining with MPD / Pearson / etc
    """
    num_sites = pam.data.shape[0]
    spd = np.zeros((num_sites, 1), dtype=float)
    for x in range(num_sites):
        sp_idxs = np.where(pam.data[x] == 1)[0]
        num_sp = len(sp_idxs)
        if num_sp > 1:
            total_distance = 0.0

            for i in range(num_sp - 1):
                for j in range(i + 1, num_sp):
                    total_distance += phylo_dist_mtx.data[i, j]

            spd[x, 0] = total_distance

    return spd

# .............................................................................
def pearson_correlation(pam, phylo_dist_mtx):
    """Calculates the Pearson correlation coef. for each site
    TODO:
    -----
        Consider combining with MPD
        
        Check for NaNs
    """
    num_sites = pam.data.shape[0]
    pearson = np.zeros((num_sites, 1), dtype=float)
    
    for s in range(num_sites):
        sp_idxs = np.where(pam.data[s] == 1)[0]
        num_sp = len(sp_idxs)
        num_pairs = num_sp * (num_sp - 1) / 2
        if num_pairs >= 2:
            # Need at least 2 pairs
            pair_dist = []
            pair_sites_shared = []
            for i in range(num_sp - 1):
                for j in range(i + 1, num_sp):
                    pair_dist.append(phylo_dist_mtx.data[i, j])
                    pair_sites_shared.append(pam.data[:, i].dot(pam.data[:, j]))
            # X : Pair distance
            # Y : Pair sites shared
            x = np.array(pair_dist)
            y = np.array(pair_sites_shared)
            sum_xy = np.sum(x*y)
            sum_x = np.sum(x)
            sum_y = np.sum(y)
            sum_x_sq = np.sum(x**2)
            sum_y_sq = np.sum(y**2)
            
            # Pearson
            p_num = sum_xy - sum_x*sum_y / num_pairs
            p_denom = np.sqrt(
                (sum_x_sq - (sum_x**2 / num_pairs)
                 ) * (sum_y_sq - (sum_y**2 / num_pairs)))
            pearson[s, 0] = p_num / p_denom
    return pearson
            
# .............................................................................
def phylogenetic_diversity(pam, tree, taxon_labels):
    """Calculate phylogenetic diversity
    """
    num_sites = pam.data.shape[0]
    pd = np.zeros((num_sites, 1), dtype=float)
    for x in range(num_sites):
        sp_idxs = np.where(pam.data[x] == 1)[0]
        if len(sp_idxs) > 1:
            present_taxa = [taxon_labels[i] for i in sp_idxs]
            mrca = tree.tree.mrca(taxon_labels=present_taxa)
            visited = set([mrca])
            total_distance = 0.0
            # TODO: Could probably get these all at once and use squids
            flt = lambda x: x.taxon is not None and x.taxon.label in present_taxa
            for node in tree.tree.find_nodes(filter_fn=flt):
            #for tax_label in taxon_labels:
                n = node
                #n = tree.find_node_with_taxon_label(tax_label)
                while n not in visited:
                    total_distance += n.edge.length
                    visited.add(n)
                    n = n.parent_node
            pd[x, 0] = total_distance
    
    return pd

