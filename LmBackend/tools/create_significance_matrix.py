"""This script creates a significance matrix

This script creates a significance matrix composed of three layers.  The first
layer will be the observed values.  The second will be the p values generated
by comparing the values of each of the permuted values matrices and the
observed values.  The values in this layer will indicate what proportion of the
values, or absolute values, from the permuted matrices are larger than the
observed.  Finally, the third layer will indicate which cells should be
considered significant after undergoing p-value correction.
"""
import argparse

from lmpy import Matrix

from LmCommon.common.ready_file import ready_filename
from LmCommon.statistics import permutation_testing as ptest


# .............................................................................
def matrix_object_generator(matrix_filenames):
    """Generator that produces Matrix objects from file names

    Args:
        matrix_filenames (:obj: `list`): A list of file names to load into
            Matrix objects
    """
    for file_name in matrix_filenames:
        yield Matrix.load(file_name)


# .............................................................................
def main():
    """Main run method for the script.
    """
    parser = argparse.ArgumentParser(
        description='Create frequency matrix for a set of permutation tests')
    parser.add_argument('observed_matrix', type=str,
                        help='File location of observed matrix')
    parser.add_argument(
        'out_matrix_filename', type=str,
        help='File location to write the output matrix')
    parser.add_argument(
        'random_matrix', type=str, nargs='+',
        help='File location of random matrix')
    parser.add_argument(
        '-a', action='store_true', help='If set, compare absolute values')
    parser.add_argument(
        '--fdr', type=float, default=0.05,
        help=('The false discovery rate, or alpha, to use when'
              ' determining significance'))
    parser.add_argument(
        '-t', '--test_mtx', type=str,
        help='Use this as the test matrix instead of the observed')
    args = parser.parse_args()

    if args.a:
        cmp_func = ptest.compare_absolute_values
    else:
        cmp_func = ptest.compare_signed_values

    obs = Matrix.load(args.observed_matrix)

    if args.test_mtx is not None:
        test_mtx = Matrix.load(args.test_mtx)
    else:
        test_mtx = obs

    p_values = ptest.get_p_values(
        test_mtx, matrix_object_generator(args.random_matrix),
        compare_func=cmp_func)

    sig_values = ptest.correct_p_values(
        p_values, false_discovery_rate=args.fdr)

    # If the last dimension has only one value, use it as the concat axis
    if obs.data.shape[-1] == 1:
        concat_axis = obs.data.ndim - 1
    else:
        concat_axis = obs.data.ndim
    out_matrix = Matrix.concatenate(
        [obs, p_values, sig_values], axis=concat_axis)

    ready_filename(args.out_matrix_filename)
    out_matrix.write(args.out_matrix_filename)


# .............................................................................
if __name__ == '__main__':
    main()
