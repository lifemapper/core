"""This script controls a species process.

The species process includes processing for occurrence data, modeling,
projecting, and presenc absence vector creation.
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
    args = parser.parse_args()
    
    sweep_config = ParameterSweepConfiguration.load(args.config_file)
    sweeper = ParameterSweep(sweep_config)
    sweeper.run()
