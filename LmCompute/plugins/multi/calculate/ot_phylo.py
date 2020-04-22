"""Module containing phylogenetic statistics

Note:
    The code is originally from Stephen Smith and has been written to work with
        Lifemapper by CJ Grady
"""
import numpy as np


# .............................................................................
def mean_nearest_taxon_distance(pam, phylo_dist_mtx):
    """Calculates the nearest neighbor distance for each site in a PAM
    """
    mntd = []
    for site in pam:
        sp_idxs = np.where(site == 1)[0]

        if len(sp_idxs) > 1:
            nearest_total = 0.0
            for i in sp_idxs:
                nearest = 99999
                for j in sp_idxs:
                    cmp_val = phylo_dist_mtx[i, j]
                    if 0.0 < cmp_val < nearest:
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
    num_sites = pam.shape[0]
    mpd = np.zeros((num_sites, 1), dtype=float)
    for idx in range(num_sites):
        sp_idxs = np.where(pam[idx] == 1)[0]
        num_sp = len(sp_idxs)
        if num_sp > 1:
            num_pairs = num_sp * (num_sp - 1) / 2
            total_distance = 0.0

            for i in range(num_sp - 1):
                for j in range(i + 1, num_sp):
                    total_distance += phylo_dist_mtx[i, j]

            mpd[idx, 0] = total_distance / num_pairs

    return mpd


# .............................................................................
def sum_pairwise_distance(pam, phylo_dist_mtx):
    """Calculates the sum pairwise distance for all species present at a site

    TODO:
        * Consider combining with MPD / Pearson / etc
    """
    num_sites = pam.shape[0]
    spd = np.zeros((num_sites, 1), dtype=float)
    for idx in range(num_sites):
        sp_idxs = np.where(pam[idx] == 1)[0]
        num_sp = len(sp_idxs)
        if num_sp > 1:
            total_distance = 0.0

            for i in range(num_sp - 1):
                for j in range(i + 1, num_sp):
                    total_distance += phylo_dist_mtx[i, j]

            spd[idx, 0] = total_distance

    return spd


# .............................................................................
def pearson_correlation(pam, phylo_dist_mtx):
    """Calculates the Pearson correlation coef. for each site

    TODO:
        * Consider combining with MPD
        * Check for NaNs
    """
    num_sites = pam.shape[0]
    pearson = np.zeros((num_sites, 1), dtype=float)

    for site_idx in range(num_sites):
        sp_idxs = np.where(pam[site_idx] == 1)[0]
        num_sp = len(sp_idxs)
        num_pairs = num_sp * (num_sp - 1) / 2
        if num_pairs >= 2:
            # Need at least 2 pairs
            pair_dist = []
            pair_sites_shared = []
            for i in range(num_sp - 1):
                for j in range(i + 1, num_sp):
                    pair_dist.append(phylo_dist_mtx[i, j])
                    pair_sites_shared.append(
                        pam[:, i].dot(pam[:, j]))
            # X : Pair distance
            # Y : Pair sites shared
            x_val = np.array(pair_dist)
            y_val = np.array(pair_sites_shared)
            sum_xy = np.sum(x_val * y_val)
            sum_x = np.sum(x_val)
            sum_y = np.sum(y_val)
            sum_x_sq = np.sum(x_val ** 2)
            sum_y_sq = np.sum(y_val ** 2)

            # Pearson
            p_num = sum_xy - sum_x * sum_y / num_pairs
            p_denom = np.sqrt(
                (sum_x_sq - (sum_x ** 2 / num_pairs)
                 ) * (sum_y_sq - (sum_y ** 2 / num_pairs)))
            pearson[site_idx, 0] = p_num / p_denom
    return pearson


# .............................................................................
def _get_node_filter(present_taxa):
    """Return a node filter for present taxa
    """

    def _node_filter(node):
        """Return a boolean indication if the node is in present taxa
        """
        return node.taxon is not None and node.taxon.label in present_taxa

    return _node_filter


# .............................................................................
def phylogenetic_diversity(pam, tree, taxon_labels):
    """Calculate phylogenetic diversity
    """
    num_sites = pam.shape[0]
    pd_mtx = np.zeros((num_sites, 1), dtype=float)
    for idx in range(num_sites):
        sp_idxs = np.where(pam[idx] == 1)[0]
        if len(sp_idxs) > 1:
            present_taxa = [taxon_labels[i] for i in sp_idxs]
            mrca = tree.tree.mrca(taxon_labels=present_taxa)
            visited = set([mrca])
            total_distance = 0.0
            flt = _get_node_filter(present_taxa)
            for node in tree.tree.find_nodes(filter_fn=flt):
                check_node = node
                # n = tree.find_node_with_taxon_label(tax_label)
                while check_node not in visited:
                    total_distance += check_node.edge.length
                    visited.add(check_node)
                    check_node = check_node.parent_node
            pd_mtx[idx, 0] = total_distance

    return pd_mtx
