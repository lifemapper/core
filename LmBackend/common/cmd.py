"""Module containing Makeflow Rule class for Lifemapper
"""
from LmBackend.common.lmobj import LMObject


# ............................................................................
class MfRule(LMObject):
    """Class to create commands for a makeflow document
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, command, targets, dependencies=None, comment=''):
        """Constructor for commands used by Makeflow

        Args:
            command (str) : A string representing the bash command for creating
                the rule (possibly prefixed with 'LOCAL ' to run on machine
                running Makeflow).
            targets (list of str) : A list of files that are created by this
                rule.  These should be relative file paths to the workspace
                where Makeflow is running.
            dependencies (list of str) : A list of relative file paths for
                dependencies that must be created before this rule can start.
                The paths should be relative to the workspace where Makeflow is
                running.
            comment (str) : A comment that can be added to a Makeflow document
                for clarity about this rule.
        """
        self.command = command
        self.targets = targets
        if dependencies is None:
            self.dependencies = []
        else:
            self.dependencies = dependencies
        self.comment = comment
