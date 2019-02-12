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
        '-p', '--pedantic', action='store_true',
        help='Run in pedantic mode where every exception fails out.')
    args = parser.parse_args()
    
    sweep_config = ParameterSweepConfiguration.load(args.config_file)
    sweeper = ParameterSweep(sweep_config, pedantic_mode=args.pedantic)
    sweeper.run()
