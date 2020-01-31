#!/bin/bash
"""This script modifies the headers of an ASCII file
"""
import argparse
import re


# .............................................................................
def _process_float_header(header_row, num_digits):
    """This method will process a header row and truncate a floats if necessary

    Args:
        header_row (str): This is a string in the format "{name}    {value}"
        num_digits (int): Truncate a decimal after this many places, keep all
            if this is None
    """
    # Split out header name and value (replace tabs with spaces and use
    #     regular expression to split
    header, value = re.split(r' +', header_row.replace('\t', ' '))
    # Truncate the value by finding the decimal (if it exists) and adding
    #    num_digits places
    truncated_value = value[:value.find('.')+num_digits+1
                           ] if value.find('.') >= 0 else value
    return '{}      {}\n'.format(header, truncated_value)


# .............................................................................
def main():
    """Main method of script
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='This script modifies ASCII headers')

    parser.add_argument(
        'in_ascii_filename', type=str,
        help='The file location of the original ASCII file')
    parser.add_argument(
        'out_ascii_filename', type=str,
        help='The file location of the modified ASCII file')
    parser.add_argument('-d', type=int, help='The number of digits to keep')

    args = parser.parse_args()

    cont = True
    with open(args.in_ascii_filename) as asc_in:
        with open(args.out_ascii_filename, 'w') as asc_out:
            for line in asc_in:
                if cont:
                    if line.lower().startswith('ncols'):
                        # This will be an integer, just write it
                        asc_out.write(line)
                    elif line.lower().startswith('nrows'):
                        # This will be an integer, just write it
                        asc_out.write(line)
                    elif line.lower().startswith('xllcorner'):
                        asc_out.write(
                            _process_float_header(line, num_digits=args.d))
                    elif line.lower().startswith('yllcorner'):
                        asc_out.write(
                            _process_float_header(line, num_digits=args.d))
                    elif line.lower().startswith('cellsize'):
                        asc_out.write(
                            _process_float_header(line, num_digits=args.d))
                    elif line.lower().startswith('dx'):
                        asc_out.write(
                            _process_float_header(
                                line, num_digits=args.d)).replace(
                                    'dx', 'cellsize')
                    elif line.lower().startswith('dy'):
                        # Cell size should be the same for now
                        pass
                    elif line.lower().startswith('nodata_value'):
                        asc_out.write(line)
                    else: # Data line
                        cont = False
                        asc_out.write(line)
                else:
                    asc_out.write(line)


if __name__ == "__main__":
    main()
