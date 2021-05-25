"""Location of local configuration options
"""
import configparser
import os

from LmCommon.common.singleton import singleton

# Looks for a Lifemapper configuration file path environment variable.  If one
#    cannot be found, raise an exception
COMPUTE_CONFIG_FILENAME = os.getenv('LIFEMAPPER_COMPUTE_CONFIG_FILE')
SERVER_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SERVER_CONFIG_FILE')
SITE_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SITE_CONFIG_FILE')


# ............................................................................
@singleton
class Config:
    """Reads configuration from base config and site config files
    """

    # .....................................
    def __init__(self, fns=None, site_fn=SITE_CONFIG_FILENAME,
                 include_defaults=True):
        """Constructor

        Note:
            * Last file specified wins

        Todo:
            * Consider making Config a subclass of SafeConfigParser
        """
        # Start a list of config files.  Begin with default config files
        file_list = []
        if include_defaults:
            file_list = [COMPUTE_CONFIG_FILENAME, SERVER_CONFIG_FILENAME]

        # Add site config files
        if site_fn is not None:
            file_list.append(site_fn)

        # Add specified config files (ex BOOM config)
        if fns is not None:
            if not isinstance(fns, list):
                fns = [fns]
            file_list.extend(fns)

        # Remove Nones if they exist
        file_list = [f for f in file_list if f is not None]

        if not file_list:
            raise ValueError(
                'Missing {} or {} environment variable'.format(
                    'LIFEMAPPER_SERVER_CONFIG_FILE',
                    'LIFEMAPPER_COMPUTE_CONFIG_FILE'))
        self.config_files = file_list
        self.reload()

    # .....................................
    def get(self, section, item):
        """Wrapper around config get
        """
        return self.config.get(section, item)

    # .....................................
    def getboolean(self, section, item):
        """Wrapper around config getboolean
        """
        return self.config.getboolean(section, item)

    # .....................................
    def getfloat(self, section, item):
        """Wrapper around config getfloat
        """
        return self.config.getfloat(section, item)

    # .....................................
    def getint(self, section, item):
        """Wrapper around config getint
        """
        return self.config.getint(section, item)

    # .....................................
    def getlist(self, section, item):
        """Get a list of items from config
        """
        list_str = self.config.get(section, item).strip('[').strip(']')
        return [itm.strip() for itm in list_str.split(',')]

    # .....................................
    def getsections(self, section_prefix):
        """Get the sections in the configuration
        """
        matching = []
        for section in self.config.sections():
            if section.startswith(section_prefix):
                matching.append(section)
        return matching

    # .....................................
    def getoptions(self, section):
        """Get the options from config
        """
        return self.config.options(section)

    # .....................................
    def reload(self):
        """Reload the configuration to catch any updates.
        """
        self.config = configparser.SafeConfigParser()
        read_config_files = self.config.read(self.config_files)
        if not read_config_files:
            raise ValueError(
                'No config files found matching {}'.format(
                    self.config_files))
