"""
@summary: Copy of module from Stephen for PD stats
@note: Probably want to remove this in favor of a copy of the stats repository
"""
import sys

import LmCompute.plugins.multi.calculate.tree_reader as tree_reader
import LmCompute.plugins.multi.calculate.tree_utils as tree_utils
#import tree_reader
#import tree_utils

# phylogenetic diversity: sum of the branch lengths for the 
# minimum spanning path of the involved taxa
# tree is the root from the newick
# tips is a list of the tips (node objects) to get the value for
def pd(tree,tips):
    traveled = set()
    traveled.add(tree_utils.get_mrca(list(tips),tree))
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

"""
this main is not checking lots of error things because it is just a
demo of how to use the function
"""
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "python "+sys.argv[0]+" newick.tre comma_seperated_taxa"
        sys.exit(0)
    
    tree = tree_reader.read_tree_file_iter(sys.argv[1]).next()
    ndsdict = {} # key is name, value is node object
    for i in tree.leaves():
        ndsdict[i.label] = i

    taxa = sys.argv[2].split(",")
    tips = set()
    for i in taxa:
        try:
            tips.add(ndsdict[i])
        except:
            print i,"not in tree"
            print "exiting..."
            sys.exit(1)

    print "pd:",pd(tree,tips)