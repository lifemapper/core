Required Keys:
******************

ARCHIVE_USER
ARCHIVE_NAME

SCENARIO_PACKAGE
SCENARIO_PACKAGE_PROJECTION_SCENARIOS
SCENARIO_PACKAGE_MODEL_SCENARIO
MAPUNITS
EPSG

DATA_SOURCE (GBIF, USER, EXISTING, TAXON_IDS, TAXON_NAMES)
  if USER or GBIF, need OCC_DATA_NAME
                        OCC_DATA_DELIMITER
  if EXISTING then need OCC_ID_FILENAME
  if TAXON_IDS then need  TAXON_ID_FILENAME


Optional Keys (filled by defaults):
***********************************
ARCHIVE_PRIORITY
TROUBLESHOOTERS

OCC_EXP_MJD
# Expiration date triggering re-query and computation
OCC_EXP_YEAR
OCC_EXP_MONTH
OCC_EXP_DAY
POINT_COUNT_MIN

# .........................................      
ASSEMBLE_PAMS
GRID_NUM_SIDES
GRID_CELL_SIZE
GRID_BBOX
GRID_NAME

# Intersection params
INTERSECT_FILTER_STRING
INTERSECT_MAX_PRESENCE
INTERSECT_MIN_PERCENT
INTERSECT_MIN_PRESENCE
INTERSECT_VAL_NAME

COMPUTE_PAM_STATS
NUM_PERMUTATIONS

# .........................................      
COMPUTE_MCPA 
BIOGEO_HYPOTHESES_LAYERS
TREE
   
