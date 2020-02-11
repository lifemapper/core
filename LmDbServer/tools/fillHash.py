"""
"""
import os

from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe


# ...............................................
def _updateLayer(lyr, scribe):
    dloc = lyr.getDLocation()
    lyrid = lyr.get_id()
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
        print(('Pulling User {} layers 0 through {}'.format(usr, total)))
        envlyrs = scribe.listLayers(0, total, userId=usr, atom=False)
        for elyr in envlyrs:
            _updateLayer(elyr, scribe)
