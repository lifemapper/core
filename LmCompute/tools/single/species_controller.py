"""This script controls a species process.

The species process includes processing for occurrence data, modeling,
projecting, and presence absence vector creation.
"""
import argparse

from LmBackend.common.parameter_sweep_config import ParameterSweepConfiguration
from LmCompute.plugins.single.controller.parameter_sweep import ParameterSweep


# .............................................................................
def main():
    """Main method for script
    """
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


# .............................................................................
if __name__ == '__main__':
    main()
