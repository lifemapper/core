"""Module containing MaxEnt constants
"""
# TODO: Remove or resolve with LmCompute/common/lmconstants
MAXENT_MODEL_TOOL = 'density.MaxEnt'
MAXENT_PROJECT_TOOL = 'density.Project'
MAXENT_CONVERT_TOOL = 'density.Convert'
MAXENT_VERSION = '3.4.1'

DEFAULT_MAXENT_OPTIONS = [
    'nowarnings',
    'nocache',
    'autorun',
    '-z'
]

DEFAULT_MAXENT_PARAMETERS = {
    'addallsamplestobackground': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'addsamplestobackground': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'adjustsampleradius': {
        'default': '0',
        'process': lambda x: str(int(x))
    },
    'askoverwrite': {
        # This doesn't make sense without a GUI
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'appendtoresultsfile': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'allowpartialdata': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    # CJG - 06/24/2015
    # This parameter needs to be quoted so that Maxent will pick up the entire
    #      thing.  That is why there are single quotes inside of double quotes
    'applythresholdrule': {
        'default': "'None'",
        'options': {
            0: "'None'",
            1: "'Fixed cumulative value 1'",
            2: "'Fixed cumulative value 5'",
            3: "'Fixed cumulative value 10'",
            4: "'Minimum training presence'",
            5: "'10 percentile training presence'",
            6: "'Equal training sensitivity and specificity'",
            7: "'Maximum training sensitivity plus specificity'",
            8: "'Equal test sensitivity and specificity'",
            9: "'Maximum test sensitivity plus specificity'",
            10: "'Equate entropy of thresholded and origial distributions'"
        },
        'process': lambda x: x
    },
    'autofeature': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'autorun': {
        # This should always be set to true unless something changes and we
        #    have a GUI
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'beta_categorical': {
        'default': '-1.0',
        'process': lambda x: str(float(x))
    },
    'beta_hinge': {
        'default': '-1.0',
        'process': lambda x: str(float(x))
    },
    'beta_lqp': {
        'default': '-1.0',
        'process': lambda x: str(float(x))
    },
    'beta_threshold': {
        'default': '-1.0',
        'process': lambda x: str(float(x))
    },
    'betamultiplier': {
        'default': '1.0',
        'process': lambda x: str(float(x))
    },
    'biasfile': {
        # Specifies a file name of a grid of relative sampling effort
        #     We aren't ready for this
        'default': '',
        'process': str
    },
    'cache': {  # If true, create mxe files.  Experiment with this
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'convergencethreshold': {
        'default': '0.00001',
        'process': lambda x: str(float(x))
    },
    'defaultprevalence': {
        'default': '0.5',
        'process': lambda x: str(float(x))
    },
    'doclamp': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'extrapolate': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'fadebyclamping': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'hinge': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'hingethreshold': {
        'default': '15',
        'process': lambda x: str(int(x))
    },
    'jackknife': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'linear': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'logfile': {
        # The default should be fine
        'default': 'maxent.log',
        'process': lambda x: x
    },
    'logscale': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'lq2lqptthreshold': {
        'default': '80',
        'process': lambda x: str(int(x))
    },
    'l2lqthreshold': {
        'default': '10',
        'process': lambda x: str(int(x))
    },
    'maximumbackground': {
        'default': '10000',
        'process': lambda x: str(int(x))
    },
    'maximumiterations': {
        'default': '',
        'process': lambda x: str(int(x))
    },
    'nodata': {
        'default': '-9999',
        'process': lambda x: x
    },
    'outputfiletype': {
        # We rely on this being set to ASCII.  Don't change unless we change
        #    that assumption
        'default': 'asc',
        'process': lambda x: x
    },
    'outputformat': {
        'default': 'logistic',
        'options': {
            0: 'raw',
            1: 'logistic',
            2: 'cumulative',
            3: 'cloglog'
        },
        'process': lambda x: x
    },
    'outputgrids': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'perspeciesresults': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'pictures': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'plots': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'prefixes': {  # Used for samples / layers / layer types prefix
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'product': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'quadratic': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'randomseed': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'randomtestpoints': {
        'default': '0',
        'process': lambda x: str(int(x))
    },
    'removeduplicates': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'replicates': {
        'default': '1',
        'process': lambda x: str(int(x))
    },
    'replicatetype': {
        'default': 'crossvalidate',
        'options': {
            0: 'crossvalidate',
            1: 'bootstrap',
            2: 'subsample'
        },
        'process': lambda x: x
    },
    'responsecurves': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'responsecurvesexponent': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'testsamplesfile': {
        # Specifies a file name of a samples file to use for testing
        #  We aren't ready for this
        'default': '',
        'process': str
    },
    'threads': {
        # The number of threads that Maxent can use.  Experiment with this
        'default': '1',
        'process': lambda x: str(int(x))
    },
    'threshold': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'togglellayerselected': {
        # This parameter toggles only layers with this prefix to be selected
        #  We aren't prepared for this
        'default': '',
        'process': lambda x: x
    },
    'togglelayertype': {
        # This parameter toggles layers with this prefix to be categorical
        #  We aren't prepared for this
        'default': '',
        'process': lambda x: x
    },
    'togglespeciesselected': {
        # This parameter toggles only species with this prefix to be selected
        #  We aren't prepared for this
        'default': '',
        'process': lambda x: x
    },
    'tooltips': {  # Used with GUI
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'verbose': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'visible': {
        # Leave false because we don't have a windowing environment
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'warnings': {
        # Leave false because we don't have a GUI for them to pop up in
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'writebackgroundpredictions': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'writeclampgrid': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'writemess': {
        'default': 'true',
        'process': lambda x: str(bool(int(x))).lower()
    },
    'writeplotdata': {
        'default': 'false',
        'process': lambda x: str(bool(int(x))).lower()
    }
}
