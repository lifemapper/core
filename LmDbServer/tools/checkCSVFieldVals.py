run = 3
fname = 'smallsort_gbif_borneo_simple_%d.csv' % run

def checkfile(fname):
   good26 = 0
   good27 = 0
   total = 0
   ranks = set()
   f = open(fname, 'r')
   rdr = csv.reader(f, delimiter='\t')
   for row in rdr:
      total += 1
      ranks.add(row[11])
      try:
         int(row[26])
      except:
         pass
      else:
         good26 += 1
         
      try:
         int(row[27])
      except:
         pass
      else:
         good27 += 1
   f.close()
   print 'Ranks {}'.format(ranks)
   print 'Total = %d, Good26 = %d, Good27 = %d' % (total, good26, good27)
      
