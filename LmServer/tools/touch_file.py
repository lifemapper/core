"""Script to 'touch' a file and create it as necessary.
"""
import argparse

from LmBackend.common.lmobj import LMObject


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(description='This script touches a file')
    # Inputs
    parser.add_argument('file_name', type=str, help='The file path to touch')
    args = parser.parse_args()

    lmo = LMObject()
    lmo.ready_filename(args.file_name)

    with open(args.file_name, 'w') as out_f:
        out_f.write('1')


# .............................................................................
if __name__ == '__main__':
    main()
