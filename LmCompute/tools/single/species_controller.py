"""This script controls a species process.

The species process includes processing for occurrence data, modeling,
projecting, and presence absence vector creation.
"""
import argparse

from LmBackend.common.parameter_sweep_config import ParameterSweepConfiguration
from LmCompute.plugins.single.controller.parameter_sweep import ParameterSweep

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Species parameter sweep')
    parser.add_argument(
        'config_file', type=str,
        help='A configuration file for this parameter sweep')
    parser.add_argument(
        '-b', '--base_work_dir', type=str,
        help='A base work directory where computations should run.')
    parser.add_argument(
        '-p', '--pedantic', action='store_true',
        help='Run in pedantic mode where every exception fails out.')
    args = parser.parse_args()
    
    sweep_config = ParameterSweepConfiguration.load(args.config_file)
    sweeper = ParameterSweep(
        sweep_config, base_work_dir=args.base_work_dir,
        pedantic_mode=args.pedantic)
    sweeper.run()

"""
from LmBackend.common.parameter_sweep_config import ParameterSweepConfiguration
from LmCompute.plugins.single.controller.parameter_sweep import ParameterSweep
from exceptions import ZeroDivisionError
import json
import os
from osgeo import ogr
from time import sleep

import LmBackend.common.layerTools as layer_tools
from LmBackend.common.lmconstants import MaskMethod, RegistryKey

from LmCommon.common.lmconstants import ProcessType, JobStatus, LMFormat
from LmCommon.common.readyfile import readyFilename
from LmCommon.encoding.layer_encoder import LayerEncoder

from LmCompute.common.log import LmComputeLogger
import LmCompute.plugins.single.mask.create_mask as create_mask
from LmCompute.plugins.single.modeling.maxent import MaxentWrapper
from LmCompute.plugins.single.modeling.openModeller import OpenModellerWrapper
from LmCompute.plugins.single.occurrences.csvOcc import *


config_file = '/share/lm/data/archive/kubi/000/001/139/212/species_config_1139212.json' 
sweep_config = ParameterSweepConfiguration.load(config_file)
sweeper = ParameterSweep(sweep_config)

self = sweeper

occ_config = self.sweep_config.get_occurrence_set_config().next()

# for occ_config in self.sweep_config.get_occurrence_set_config():
(process_type, occ_set_id, url_fn_or_key, out_file, big_out_file,
 max_points, metadata) = occ_config

occ_metrics = None
occ_snippets = None

is_gbif = False
# if process_type in (ProcessType.USER_TAXA_OCCURRENCE,
#                     ProcessType.GBIF_TAXA_OCCURRENCE):
if process_type == ProcessType.GBIF_TAXA_OCCURRENCE:
    is_gbif = True

# status = createShapefileFromCSV(url_fn_or_key, metadata, 
#                         out_file, big_out_file, max_points, 
#                         is_gbif=is_gbif, log=self.log)

log = self.log
(csv_fname, metadata, out_fname, big_fname) = (url_fn_or_key, 
            metadata, out_file, big_out_file)

readyFilename(out_fname, overwrite=True)
readyFilename(big_fname, overwrite=True)

csvreader, f = _getCSVReader(csv_fname, delimiter)
lines = []
ln, recno = _getLine(csvreader, 0)
while ln is not None:
    lines.append(ln)
    ln, recno = _getLine(csvreader, recno)
f.close()


with open(csv_fname) as inF:
    lines = inF.readlines()

# remove non-encodeable lines
cleanLines = []
for ln in lines:
    try: 
        clnLn = ln.encode(ENCODING)
    except:
        pass
    else:
        cleanLines.append(clnLn)
        
ln = lines[0] 
parts = ln.split('\t')
parts
['1257817388', 'urn:catalog:MO:Tropicos:100470589', '5312309', '7bd65a7a-f762-11e1-a439-00145eb45e9a', '90fd6680-349f-11d8-aa2d-b8a03c50a862', 'PRESERVED_SPECIMEN', '6', '7707728', '196', '1169', '7689', '2807173', '5312296', 'Prescottia petiolaris Lindl.', '-16.233333', '-67.866667', '\\N', '\\N', '\\N', 'Iv\xc3\xa1n Jim\xc3\xa9nez', 'MO', 'MO', '100470589\r\n']
>>> for pt in parts:
...     try:         
...             print pt, pt.encode(ENCODING)
...     except:
...             print 'NO  ', pt

rawdata = '\n'.join(cleanLines)


self._register_output_object(
    RegistryKey.OCCURRENCE, occ_set_id, status, out_file, 
    secondary_outputs=[big_out_file], process_type=process_type,
    metrics=occ_metrics, snippets=occ_snippets)


self._create_occurrence_sets()
self._create_masks()
self._create_models()
self._create_projections()
self._create_pavs()

# Write metrics
with open(self.sweep_config.metrics_filename, 'w') as out_metrics:
    json.dump(self.get_metrics(), out_metrics)

# Write snippets
with open(self.sweep_config.snippets_filename, 'w') as out_snippets:
    json.dump(self.get_snippets(), out_snippets)

# Write stockpile information
with open(self.sweep_config.stockpile_filename, 'w') as out_stockpile:
    json.dump(self.get_stockpile_info(), out_stockpile)

# Write PAV information
with open(self.sweep_config.pavs_filename, 'w') as out_pavs:
    json.dump(self.get_pav_info(), out_pavs)


"""