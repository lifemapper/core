"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research
 
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
import os

from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe


# ...............................................
def _updateLayer(lyr, scribe):
   dloc = lyr.getDLocation()
   lyrid = lyr.getId()
   # Compute the hash
   if dloc is not None and os.path.exists(dloc):
      if lyr.verify is None:
         scribe.log.debug('Why is verify empty?')
         verify = lyr.computeHash()
         lyr._verify = verify
      if lyr.verify is not None:
         success = scribe._mal.executeModifyFunction('lm_updateLayerHash', lyrid, lyr.verify)
         if not success:
            scribe.log.debug('  Failed to update verify in Layer {}'.format(lyrid))
         else:
            scribe.log.debug('  Updated layer {} hash'.format(lyrid))
   else:
      scribe.log.debug('  Layer {} does not exist at {}'.format(lyrid, dloc))

# ...............................................
if __name__ == '__main__':
   logger = ScriptLogger('fillHash')
   scribe = Scribe(logger)
   success = scribe.openConnections()

   userObjs = scribe.getUsers()
#    userids = [usr.userid for usr in userObjs]
   userids = ['changeThinking', 'Tash_New', '321']
   
   for usr in userids:
      total = scribe.countLayers(userId=usr)
      print ('Pulling User {} layers 0 through {}'.format(usr, total))
      envlyrs = scribe.listLayers(0, total, userId=usr, atom=False)
      for elyr in envlyrs:
         _updateLayer(elyr, scribe)
