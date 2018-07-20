"""
@summary: Module containing functions for creating output package files
@author: CJ Grady
@version: 2.0
@status: alpha
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
import cherrypy
from collections import defaultdict
import json
import os
from StringIO import StringIO
import zipfile

from LmCommon.common.lmconstants import LMFormat, MatrixType, JobStatus,\
   HTTPStatus, PamStatKeys
from LmCommon.common.matrix import Matrix
from LmCommon.common.lmXml import tostring

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset

from LmWebServer.formatters.emlFormatter import makeEml
from LmWebServer.formatters.geoJsonFormatter import geoJsonify_flo


# .............................................................................
# TODO: Move to lmconstants
GRIDSET_DIR = 'gridset'
MATRIX_DIR = os.path.join(GRIDSET_DIR, 'matrix')
# TODO: Assmble from constants
STATIC_PACKAGE_DIR = '/opt/lifemapper/LmWebServer/assets/gridset_package'
DYN_PACKAGE_DIR = 'package'

# .............................................................................
def createIndexHtml(gridset_name):
   """
   @summary: Generate an index.html page for the package
   """
   return """\
<!doctype html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Lifemapper Analysis Results Package</title>
  </head>
  <body>
    <h1>Analysis Results for gridset {gridset_name}</h1>
    <h3>These results were generated by the Biotaphy project, a collaboration of</h3>
    <table>
      <tr>
        <td>
          <img src="./images/idigbio_logo.png" alt="iDigBio" />
        </td>
        <td>
          <img src="./images/lm_logo.png" alt="Lifemapper" />
        </td>
        <td>
          <img src="./images/otl_logo.png" alt="Open Tree of Life" />
        </td>
      </tr>
      <tr>
        <td style="text-align: center;">
          iDigBio
        </td>
        <td style="text-align: center;">
          Lifemapper
        </td>
        <td style="text-align: center;">
          Open Tree of Life
        </td>
      </tr>
    </table>
    <br />
    <p>This package contains the following:</p>
    <table>
      <tr>
        <td>
          <img src="./images/mcpa_thumb.png" alt="MCPA Thumbnail" />
        </td>
        <td>
          <img src="./images/site-maps_thumb.png" alt="Site Maps" />
        </td>
        <td>
          <img src="./images/scatterplot_thumb.png" alt="Scatterplots Thumbnail" />
        </td>
      </tr>
      <tr>
        <td style="text-align: center;">
           <a href="./mcpa.html">MCPA</a>
        </td>
        <td style="text-align: center;">
           <a href="./site-maps.html">Site Statistic Maps</a>
        </td>
        <td style="text-align: center;">
           <a href="./stats.html">Scatterplots</a>
        </td>
      </tr>
      <!--
      <tr>
        <td style="text-align: center;">
          <a href="./help_mcpa.html">MCPA Help</a>
        </td>
        <td style="text-align: center;">
          <a href="./help_site-maps.html">Site Statistic Maps Help</a>
        </td>
        <td style="text-align: center;">
          <a href="./help_scatterplots.html">Scatterplots Help</a>
        </td>
      </tr>
      -->
    </table>
    <p>* Due to the size of the data these pages may take some time to load.</p>
    <h4>The Biotaphy project is supported by NSF BIO Award #1458422.</h4>
  </body>
</html>""".format(gridset_name=gridset_name)
   
# .............................................................................
def createStatsMeta():
   """
   @summary: Create a statistic metadata lookup for all stats
   """
   return {
      PamStatKeys.ALPHA : {
         'name' : 'Alpha Diversity',
         'description' : 'Alpha Diversity is the species richness (number of species present) per site'
      },
      PamStatKeys.ALPHA_PROP : {
         'name' : 'Proportional Alpha Diversity',
         'description' : 'Proportional Alpha Diversity is the proportion of the entire population of species that are present per site'
      },
      PamStatKeys.PHI : {
         'name' : 'Range Size Per Site',
         'description' : 'Range Size per site is the sum of the range sizes of each species present at each site'
      },
      PamStatKeys.PHI_AVG_PROP : {
         'name' : 'Proportional Range Size Per Site',
         'description' : 'Proportional range size per site is the sum of the range sizes at each site as a proportion of the sum of the ranges of all species in the study pool'
      },
      PamStatKeys.MNTD : {
         'name' : 'Mean Nearest Taxon Distance',
         'description' : 'Mean Nearest Taxon Distance is the average of the distance between each present species and the (phylogenetically) nearest present species for each site'
      },
      PamStatKeys.MPD : {
         'name' : 'Mean Pairwise Distance',
         'description' : 'Mean pairwise distance is the average phylogenetic distance between all species present at each site'
      },
      PamStatKeys.PEARSON : {
         'name' : "Pearson's Correlation Coefficient",
         'description' : ''
      },
      PamStatKeys.PD : {
         'name' : 'Phylogenetic Diversity',
         'description' : 'Phylogenetic Diversity is the sum of the branch lengths for the minimum spanning tree for all species at a site'
      },
      PamStatKeys.MNND : {
         'name' : 'Mean Nearest Neighbor Distance',
         'description' : 'Mean nearest neighbor distance is the average phylogenetic distance to the nearest neighbor of each species present at a site'
      },
      PamStatKeys.MPHYLODIST : {
         'name' : 'Mean Phylogenetic Distance',
         'description' : 'Mean phylogenetic distance is the average phylogenetic distance between all species present at a site'
      },
      PamStatKeys.SPD : {
         'name' : 'Sum of Phylogenetic Distance',
         'description' : 'Sum of phylogenetic distance is the total phylogenetic distance between all species present at a site'
      },
      PamStatKeys.OMEGA : {
         'name' : 'Species Range Size',
         'description' : 'Species range size is the number of sites where each species is present'
      },
      PamStatKeys.OMEGA_PROP : {
         'name' : 'Proportional Species Range Size',
         'description' : 'Proportional species range size the the proportion of the number of sites where each species is present to the total number of sites in the study area'
      },
      # TODO: Verify and clarify
      PamStatKeys.PSI : {
         'name' : 'Species Range Richness',
         'description' : 'Species range richness is the sum of the range sizes of all of the species present at a site'
      },
      # TODO: Verify and clarify
      PamStatKeys.PSI_AVG_PROP : {
         'name' : 'Mean Proportional Species Diversity',
         'description' : 'Mean Proportional Species Diversity is the average species range richness proportional to the total species range richness'
      },
      PamStatKeys.WHITTAKERS_BETA : {
         'name' : 'Whittaker\'s Beta Diversity',
         'description' : 'Whittaker\'s Beta Diversity'
      },
      PamStatKeys.LANDES_ADDATIVE_BETA : {
         'name' : 'Landes Addative Beta Diveristy',
         'description' : 'Landes Addative Beta Diversity'
      },
      PamStatKeys.LEGENDRES_BETA : {
         'name' : 'Legendres Beta Diversity',
         'description' : 'Legendres Beta Diversity'
      },
      PamStatKeys.SITES_COVARIANCE : {
         'name' : 'Sites Covariance',
         'description' : 'Sites covariance'
      },
      PamStatKeys.SPECIES_COVARIANCE : {
         'name' : 'Species Covariance',
         'description' : 'Species covariance'
      },
      PamStatKeys.SPECIES_VARIANCE_RATIO : {
         'name' : 'Schluter\'s Species Variance Ratio',
         'description' : 'Schluter\'s species covariance'
      },
      PamStatKeys.SITES_VARIANCE_RATIO : {
         'name' : 'Schluter\'s Sites Variance Ratio',
         'description' : 'Schluter\'s sites covariance'
      }
   }
   pass

# .............................................................................
def createStatHeaderLookup():
   """
   @summary: Create a statistic header lookup for all possible stats
   """
   return {
      PamStatKeys.ALPHA : 'Alpha Diversity',
      PamStatKeys.ALPHA_PROP : 'Proportional Alpha Diversity',
      PamStatKeys.PHI : 'Range Size Per Site',
      PamStatKeys.PHI_AVG_PROP : 'Proportional Range Size Per Site',
      PamStatKeys.MNTD : 'Mean Nearest Taxon Distance',
      PamStatKeys.MPD : 'Mean Pairwise Distance',
      PamStatKeys.PEARSON : "Pearson's Correlation Coefficient",
      PamStatKeys.PD : 'Phylogenetic Diversity',
      PamStatKeys.MNND : 'Mean Nearest Neighbor Distance',
      PamStatKeys.MPHYLODIST : 'Mean Phylogenetic Distance',
      PamStatKeys.SPD : 'Sum of Phylogenetic Distance',
      PamStatKeys.OMEGA : 'Species Range Size',
      PamStatKeys.OMEGA_PROP : 'Proportional Species Range Size',
      PamStatKeys.PSI : 'Species Range Richness',
      PamStatKeys.PSI_AVG_PROP : 'Proportional Species Range Richness',
      PamStatKeys.WHITTAKERS_BETA : 'Whittaker\'s Beta Diversity',
      PamStatKeys.LANDES_ADDATIVE_BETA : 'Landes Addative Beta Diveristy',
      PamStatKeys.LEGENDRES_BETA : 'Legendres Beta Diversity',
      PamStatKeys.SITES_COVARIANCE : 'Sites Covariance',
      PamStatKeys.SPECIES_COVARIANCE : 'Species Covariance',
      PamStatKeys.SPECIES_VARIANCE_RATIO : 'Schluter\'s Species Variance Ratio',
      PamStatKeys.SITES_VARIANCE_RATIO : 'Schluter\'s Sites Variance Ratio'
   }

# .............................................................................
def createHeaderLookup(headers, squids=False, scribe=None, userId=None):
   """
   @summary: Generate a header lookup to be included in the package metadata
   """
   def getHeaderDict(header, idx):
      return {
         'header' : header,
         'index' : idx
      }
      
   def getSquidHeaderDict(header, idx, scribe, userId):
      taxon = scribe.getTaxon(squid=header, userId=userId)
      ret = getHeaderDict(header, idx)
      
      for attrib, key in [('scientificName', 'scientific_name'),
                          ('canonicalName', 'canonical_name'),
                          ('rank', 'taxon_rank'),
                          ('kingdom', 'taxon_kingdom'),
                          ('phylum', 'taxon_phylum'),
                          ('txClass', 'taxon_class'),
                          ('txOrder', 'taxon_order'),
                          ('family', 'taxon_family'),
                          ('genus', 'taxon_genus')
                         ]:
         val = getattr(taxon, attrib)
         if val is not None:
            ret[key] = val
      return ret
   
      
   if squids and scribe and userId:
      return [getSquidHeaderDict(
                  headers[i], i, scribe, userId) for i in xrange(len(headers))]
   else:
      return [getHeaderDict(headers[i], i) for i in xrange(len(headers))]

# .............................................................................
def mung(data):
   """
   @summary: Replace a list of values with a map from the non-zero values to
                the indexes at which they occur
   """
   munged = defaultdict(list)
   for i, datum in enumerate(data):
      if datum != 0:
         munged[datum].append(i)
   return munged

# .............................................................................
def gridsetPackageFormatter(gsObj, includeCSV=False, includeSDM=False, 
                            stream=False):
   """
   @summary: Create a Gridset download package for the user to explore locally
   """
   if isinstance(gsObj, Gridset):
      
      # Get scribe and user id (used for creating lookup)
      scribe = BorgScribe(LmPublicLogger())
      userId = gsObj.getUserId()
      
      pkgFn = gsObj.getPackageLocation()
      
      # Look to see if the package already exists
      if not os.path.exists(pkgFn):
         # If it doesn't exist, see if we can create it
         if all([
                mtx.status >= JobStatus.COMPLETE for mtx in gsObj.getMatrices()
               ]):
            
            with zipfile.ZipFile(pkgFn, mode='w', 
                    compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zipF:
            
               for f_dir, _, fns in os.walk(STATIC_PACKAGE_DIR):
                  for fn in fns:
                     # Get relative and absolute paths for packaging
                     a_path = os.path.join(f_dir, fn)
                     r_path = a_path.replace(STATIC_PACKAGE_DIR, '')
                     zipF.write(a_path, r_path)
               
               # Write gridset objects
               gsEml = tostring(makeEml(gsObj))
               zipF.writestr(os.path.join(GRIDSET_DIR, 'gridset_{}.eml'.format(
                  gsObj.getId())), gsEml)
               
               # Write tree
               if gsObj.tree is not None:
                  with open(gsObj.tree.getDLocation()) as treeIn:
                     treeStr = treeIn.read()
                     
                     zipF.writestr(os.path.join(DYN_PACKAGE_DIR, 'tree.js'),
                                   'var taxonTree = `{}`;'.format(treeStr))
                     
                     zipF.writestr(os.path.join(GRIDSET_DIR, 'tree.tre'), treeStr)
                  treeStr = None
               
               # Index.html page
               indexFn = 'index.html'
               try:
                  gsName = gsObj.name
               except:
                  gsName = 'Gridset {}'.format(gsObj.getId())
               zipF.writestr(indexFn, createIndexHtml(gsName))

               # Matrices
               sg = gsObj.getShapegrid()
               matrices = gsObj.getMatrices()
               
               statLookupFn = os.path.join(DYN_PACKAGE_DIR, 
                                           'statNameLookup.json')
               zipF.writestr(statLookupFn, 'var statNameLookup =\n{}'.format(
                                            json.dumps(createStatsMeta(),
                                                            indent=3)))
               for mtx in matrices:
                  if mtx.status == JobStatus.COMPLETE:
                     mtxObj = Matrix.load(mtx.getDLocation())
                     
                     # Only add current matrices
                     # TODO: Change in the future
                     if mtx.dateCode == 'Curr':
                        
                        if mtx.matrixType in [MatrixType.PAM, MatrixType.ROLLING_PAM]:
                           
                           # TODO: Only do this for observed data
                           # Write SQUID lookup
                           squidLookupFn = os.path.join(DYN_PACKAGE_DIR, 'squidLookup.json')
                           zipF.writestr(squidLookupFn, 'var squidLookup =\n{}'.format(
                                                         json.dumps(createHeaderLookup(
                                                               mtxObj.getColumnHeaders(), 
                                                               squids=True, 
                                                               scribe=scribe, 
                                                               userId=userId),
                                                            indent=3)))
                           
                           mtxStr = StringIO()
                           geoJsonify_flo(mtxStr, sg.getDLocation(), matrix=mtxObj, 
                                          mtxJoinAttrib=0, ident=0, 
                                          headerLookupFilename=squidLookupFn, 
                                          transform=mung)
                           mtxStr.seek(0)
                           
                           pamPkgFn = os.path.join(DYN_PACKAGE_DIR, 'pam.js')
                           zipF.writestr(pamPkgFn, "var pam = JSON.parse('{}');".format(
                                                                        mtxStr.getvalue()))
                           # Save memory
                           mtxStr = None
                           
                           csvMtxFn = os.path.join(MATRIX_DIR, 'pam_{}{}'.format(
                                                         mtx.getId(), LMFormat.CSV.ext))
                           
                        elif mtx.matrixType == MatrixType.ANC_PAM:
                           
                           # TODO: Only do this for observed data?
                           # Write node lookup
                           nodeLookupFn = os.path.join(DYN_PACKAGE_DIR, 'nodeLookup.js')
                           zipF.writestr(nodeLookupFn, 'var nodeLookup = \n{}'.format(
                                                         json.dumps(createHeaderLookup(
                                                               mtxObj.getColumnHeaders()),
                                                            indent=3)))
            
                           mtxStr = StringIO()
                           geoJsonify_flo(mtxStr, sg.getDLocation(), matrix=mtxObj, 
                                          mtxJoinAttrib=0, ident=0, 
                                          headerLookupFilename=nodeLookupFn, 
                                          transform=mung)
                           mtxStr.seek(0)
                           
                           ancPamPkgFn = os.path.join(DYN_PACKAGE_DIR, 'ancPam.js')
                           zipF.writestr(ancPamPkgFn, "var ancPam = JSON.parse(`{}`);".format(
                                                                        mtxStr.getvalue()))
                           # Save memory
                           mtxStr = None
                           
                           csvMtxFn = os.path.join(MATRIX_DIR, 'ancPam_{}{}'.format(
                                                         mtx.getId(), LMFormat.CSV.ext))
                           
            
                        elif mtx.matrixType in [MatrixType.SITES_COV_OBSERVED, 
                                                MatrixType.SITES_COV_RANDOM, 
                                                MatrixType.SITES_OBSERVED, 
                                                MatrixType.SITES_RANDOM]:
            
                           # TODO: Only do this for observed data?
                           if mtx.matrixType == MatrixType.SITES_COV_OBSERVED:
                              mtxName = 'sitesCovarianceObserved'
                           elif mtx.matrixType == MatrixType.SITES_COV_RANDOM:
                              mtxName = 'sitesCovarianceRandom'
                           elif mtx.matrixType == MatrixType.SITES_OBSERVED:
                              mtxName = 'sitesObserved'
                           else:
                              mtxName = 'sitesRandom'        
            
                           mtxStr = StringIO()
                           # TODO: Determine if we need to mung this data
                           geoJsonify_flo(mtxStr, sg.getDLocation(), matrix=mtxObj, 
                                          mtxJoinAttrib=0, ident=0)
                           mtxStr.seek(0)
                           
                           mtxPkgFn = os.path.join(DYN_PACKAGE_DIR, '{}.js'.format(
                                                                                 mtxName))
                           zipF.writestr(mtxPkgFn, "var {} = JSON.parse(`{}`);".format(
                                                               mtxName, mtxStr.getvalue()))
                           # Save memory
                           mtxStr = None
                           
                           csvMtxFn = os.path.join(MATRIX_DIR, '{}_{}{}'.format(mtxName,
                                                         mtx.getId(), LMFormat.CSV.ext))
                           
                           
                        elif mtx.matrixType == MatrixType.MCPA_OUTPUTS:
                           # TODO: Only do this for observed data
                           csvMtxStr = StringIO()
                           mtxObj.writeCSV(csvMtxStr)
                           csvMtxStr.seek(0)
                           mcpaPkgFn = os.path.join(DYN_PACKAGE_DIR, 'mcpaMatrix.js')
                           zipF.writestr(mcpaPkgFn, 'var mcpaMatrix = `{}`;'.format(
                                                                     csvMtxStr.getvalue()))
                           
                           csvMtxFn = os.path.join(MATRIX_DIR, 'mcpa_{}{}'.format(
                                                         mtx.getId(), LMFormat.CSV.ext))
                           
                        else:
                           csvMtxFn = os.path.join(MATRIX_DIR, 
                                 '{}{}'.format(
                                    os.path.splitext(
                                       os.path.basename(
                                          mtx.getDLocation()))[0], 
                                                 LMFormat.CSV.ext))
            
                        # Write the Matrix CSV file if desired
                        if includeCSV:
                           csvMtxStr = StringIO()
                           mtxObj.writeCSV(csvMtxStr)
                           csvMtxStr.seek(0)
                           zipF.writestr(csvMtxFn, csvMtxStr.getvalue())
            
         else:
            cherrypy.response.status = HTTPStatus.ACCEPTED
            return
            
      # Package now exists, return it
      outPackageName = 'gridset-{}-package.zip'.format(gsObj.getId())
      cherrypy.response.headers['Content-Disposition'
                        ] = 'attachment; filename="{}"'.format(outPackageName)
      cherrypy.response.headers['Content-Type'] = LMFormat.ZIP.getMimeType()

      if stream:
         cherrypy.lib.file_generator(open(pkgFn, 'r'))
      else:
         with open(pkgFn) as inF:
            cnt = inF.read()
         return cnt
         
   else:
      raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 
                               'Only gridsets can be formatted as package')
