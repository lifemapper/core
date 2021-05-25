"""Converts the layers in a package directory to ASCIIs and MXEs
"""
import argparse
import os

from LmBackend.common.layer_tools import convert_layers_in_dir

DESCRIPTION = """\
This script converts all of the layers in a directory to ASCIIs and MXEs"""


# .............................................................................
def main():
    """Main method for script.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument(
        'layer_directory', type=str, help='The directory with layer rasters')
    args = parser.parse_args()

    if os.path.exists(args.layer_directory):
        convert_layers_in_dir(args.layer_directory)
    else:
        raise Exception(
            'Layer directory: {}, does not exist'.format(args.layer_directory))


# .............................................................................
if __name__ == '__main__':
    main()
