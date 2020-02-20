"""This module pings the open tree server to see if it is currently up.
"""
import random

from ot_service_wrapper import get_ottids_from_gbifids, induced_subtree

from LmServer.common.localconstants import TROUBLESHOOTERS
from LmServer.notifications.email import EmailNotifier

GBIF_IDS = [
    1008601, 1008622, 1008754, 1023454, 1029509, 1035200, 1040592, 1044951,
    1048389, 1053664, 1056860, 1057466, 1057922, 1065644, 1091412, 1095275,
    1095412, 1095869, 1105187, 1112232, 1123836, 1127535, 1132187, 1165216,
    1178180, 1227400, 1246783, 1265564, 1287795, 1296584, 1297120, 1306471,
    1314162, 1321007, 1323763, 1336365, 1338635, 1341995, 1343373, 1348562,
    1349283, 1357795, 1357958, 1364419, 1381437, 1381494, 1405052, 1406051,
    1416612, 1420066, 1427573, 1428595, 1428935, 1433531, 1434588, 1435938,
    1442099, 1442230, 1442382, 1447082, 1519013, 1528431, 1530455, 1546799,
    1553326, 1555205, 1584891, 1622099, 1626085, 1639339, 1658605, 1664382,
    1664778, 1669116, 1673377, 1729500, 1731906, 1736839, 1763742, 1772045,
    1772116, 1772716, 1784384, 1804727, 1827449, 1829823, 1837158, 1841755,
    1859014, 1862185, 1863109, 1865990, 1883891, 1884779, 1888913, 1892511,
    1897819, 1902434, 1905179, 1909491
]


# .............................................................................
def get_gbif_id_subset():
    """Gets a random subset of GBIF ids
    """
    random.shuffle(GBIF_IDS)
    return GBIF_IDS[:random.randint(3, len(GBIF_IDS))]


# .............................................................................
def report_failure(msg):
    """Report failure to Lifemapper troubleshooters
    """
    notifier = EmailNotifier()
    notifier.send_message(TROUBLESHOOTERS, 'Open Tree Service failure', msg)


# .............................................................................
def main():
    """Main method of script
    """
    # Get subset of gbif ids
    test_ids = get_gbif_id_subset()
    # ping open tree
    try:
        id_map = get_ottids_from_gbifids(test_ids)
        ott_ids = []
        for _, val in id_map.items():
            if val is not None:
                ott_ids.append(val)
        # Get the tree
        induced_subtree(ott_ids)
        print('Success')
    except Exception as e:
        msg = 'Open Tree failure:<br />{}<br />Test GBIF ids: {}'.format(
            str(e), test_ids)
        report_failure(msg)
        print('Failure')


# .............................................................................
if __name__ == '__main__':
    main()
