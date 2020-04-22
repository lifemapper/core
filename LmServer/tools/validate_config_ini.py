"""This script validates a boom config ini file
"""
import argparse

from LmCommon.common.lmconstants import (
    BoomKeys, COMPUTE_CMDS_HEADING, COMPUTE_CONTACT_HEADING,
    COMPUTE_ENV_HEADING, COMPUTE_OPTIONS_HEADING, COMPUTE_METRICS_HEADING,
    COMPUTE_ME_PLUGIN_HEADING, SERVER_BOOM_HEADING, SERVER_DB_HEADING,
    SERVER_ENV_HEADING, SERVER_MATT_DAEMON_HEADING, SERVER_PIPELINE_HEADING,
    SERVER_SDM_MASK_HEADING_PREFIX, ENCODING)
from LmServer.common.lmconstants import Algorithms

VALID_ALG_PARAMS = []
for alg_code in Algorithms.codes():
    try:
        alg = getattr(Algorithms, alg_code)
        VALID_ALG_PARAMS.extend([k.lower() for k in alg.parameters.keys()])
    except Exception:
        pass

VALID_BOOM_ENTRIES = []
for k in dir(BoomKeys):
    if not k.startswith('_'):
        VALID_BOOM_ENTRIES.append(getattr(BoomKeys, k).lower())

VALID_HEADINGS = [h.lower() for h in [
    COMPUTE_CMDS_HEADING, COMPUTE_CONTACT_HEADING, COMPUTE_ENV_HEADING,
    COMPUTE_OPTIONS_HEADING, COMPUTE_ME_PLUGIN_HEADING,
    COMPUTE_METRICS_HEADING, SERVER_BOOM_HEADING, SERVER_DB_HEADING,
    SERVER_ENV_HEADING, SERVER_MATT_DAEMON_HEADING, SERVER_PIPELINE_HEADING,
    SERVER_SDM_MASK_HEADING_PREFIX]]


# .............................................................................
def get_heading(heading_line):
    """Get the heading from a config file line
    """
    return heading_line.split('[')[1].split(']')[0].lower()


# .............................................................................
def get_name_val(entry_line):
    """Get the parameter name and a value for an entry
    """
    try:
        name, val = entry_line.split(' : ')
    except Exception:
        name, val = entry_line.split(' = ')
    return (name.lower(), val)


# .............................................................................
def get_validate_method_for_heading(heading):
    """Gets the validate method to use for a heading
    """
    if heading.lower().find('algorithm') >= 0:
        return validate_algorithm_parameter

    return validate_boom_entry


# .............................................................................
def validate_heading(heading):
    """Validate a heading
    """
    if heading.find('algorithm') >= 0 or heading in VALID_HEADINGS:
        return True

    raise Exception('Invalid heading: {}'.format(heading))


# .............................................................................
def validate_boom_entry(name, value=None):
    """Validate a boom entry
    """
    if name in VALID_BOOM_ENTRIES:
        return True

    raise Exception('Invalid boom entry: {} - {}'.format(name, value))


# .............................................................................
def validate_algorithm_parameter(name, value=None):
    """Validate an algorithm parameter
    """
    if name == BoomKeys.ALG_CODE.lower() or name in VALID_ALG_PARAMS:
        return True

    raise Exception(
        'Invalid algorithm parameter: {} - {}'.format(name, value))


# .............................................................................
def main():
    """Main method of script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('config_filename')
    args = parser.parse_args()

    with open(args.config_filename, 'r', encoding=ENCODING) as in_file:
        validate_method = validate_boom_entry
        for line in in_file:
            if line.startswith('['):
                heading = get_heading(line)
                validate_heading(heading)
                validate_method = get_validate_method_for_heading(heading)
            elif len(line.strip()) > 0:
                param_name, param_value = get_name_val(line)
                validate_method(param_name, value=param_value)
    print('\n\n\nThis config file appears to be valid!')


# .............................................................................
if __name__ == '__main__':
    main()
