"""Script to convert a raster from one format to another
"""
import argparse

from LmBackend.common.layer_tools import convert_tiff_to_ascii


# .............................................................................
def main():
    """Main method for script.
    """
    parser = argparse.ArgumentParser(
        description='This script converts a tiff to an ascii')

    parser.add_argument('tiff_fn', type=str)
    parser.add_argument('ascii_fn', type=str)
    args = parser.parse_args()

    convert_tiff_to_ascii(args.tiff_fn, args.ascii_fn)


# .............................................................................
if __name__ == '__main__':
    main()
