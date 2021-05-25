"""Module providing functions for (de)compressing lists of binary integers.

Note:
    We are using this for compressing / decompressing matrix columns with the
        Lifemapper Global PAM solr index.  It only works with PAVs.
"""
VERSION = 1


# .............................................................................
def compress(lst):
    """Compresses a list of zeros and ones into a string of run lengths
    """
    runs = []
    start_val = int(lst[0])
    val = start_val
    run_length = 0

    for i in lst:
        # If we have the same value, extend the run
        if i == val:
            run_length += 1
        else:
            # Add the run length and switch to the other value
            runs.append(str(run_length))
            val = i
            run_length = 1
    runs.append(str(run_length))
    return 'v{}s{} {}'.format(VERSION, start_val, ' '.join(runs))


# .............................................................................
def decompress(compressed_list_str):
    """Decompress a string of run lengths into a list of binary values
    """
    vals = []
    parts = compressed_list_str.split(' ')
    header = parts[0]
    # Uncomment if we end up using version information
    # version = int(header.split('s')[0].split('v')[1])
    start_val = int(header.split('s')[1])
    val = start_val

    for run_len in parts[1:]:
        vals.extend(int(run_len) * [val])
        # Will change 0 to 1 and 1 to 0
        val = abs(val - 1)

    return vals
