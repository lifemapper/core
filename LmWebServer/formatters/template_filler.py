"""This module contains functions for filling in templated strings
"""


# .............................................................................
class TemplateFiller:
    """Class responsible for filling templated strings
    """

    # ..............................
    def __init__(self, indicator='##', **args):
        self.indicator = indicator
        arg_dict = dict(args)
        self.param_dict = {}
        # Convert keys to lower case
        for k in list(arg_dict.keys()):
            self.param_dict[k.lower()] = arg_dict[k]

    # ..............................
    def fill_templated_string(self, template_str):
        """Fills a templated string with appropriate values.

        Args:
            template_str (str): A string with templated values determined by
                the self.indicator value.

        Returns:
            str: A string where templated values have been replaced
                appropriately.
        """
        tokens = template_str.split(self.indicator)
        return self._process_tokens(tokens)

    # ..............................
    def _process_tokens(self, tokens):
        """Process a list of tokens and return a string

        Args:
            tokens (:obj:`list` of :obj:`str`): A list of strings.  Every other
                token is something that should be processed.  The first item
                should not be.
        Returns:
            str: A string where templated values have been replaced
                appropriately.
        """
        ret_str = ''
        is_token = False
        search_token = None
        sub_tokens = None
        do_if = False
        in_if = False
        in_for = False
        for_lcv = None
        iter_key = None

        for token in tokens:
            # If this isn't a token, add it to return string
            if not is_token:
                ret_str += token
                # The next item should be a token
                is_token = True
            else:
                # If we are looking for a specific token, check for it
                if search_token is not None:
                    # If this is the search token, process the sub tokens
                    if token.lower() == search_token:
                        if in_if and do_if:
                            ret_str += self._process_tokens(sub_tokens)
                        elif in_for:
                            for i in self.param_dict[iter_key.lower()]:
                                self.param_dict[for_lcv] = i
                                ret_str += self._process_tokens(sub_tokens)
                            self.param_dict[for_lcv] = None
                        # Reset search and sub tokens
                        search_token = None
                        sub_tokens = None
                        is_token = False
                        in_for = False
                        in_if = False
                        for_lcv = None
                        iter_key = None
                    else:
                        # Add the token to the sub tokens list for processing
                        sub_tokens.append(token)
                else:
                    # Check if this is an 'IF' token
                    if token.lower().startswith('if_'):
                        # Create a sub tokens list and set search token
                        sub_tokens = []
                        search_token = 'END_{}'.format(token).lower()
                        # Set if we should process the IF by looking at args
                        do_if = self.param_dict[token.strip('IF_').lower()]
                        in_if = True
                    elif token.lower().startswith('for_'):
                        sub_tokens = []
                        search_token = 'END_{}'.format(token).lower()
                        in_for = True
                        token_parts = token.lower().split('_')
                        iter_key = token_parts[-1]
                        for_lcv = token_parts[1]
                    else:
                        # Process as a replace string
                        if token.lower().find('.') > 0:
                            token_parts = token.lower().split('.')
                            token_val = self.param_dict[
                                token_parts[0]][token_parts[1]]
                        else:
                            token_val = self.param_dict[token.lower()]
                        ret_str += token_val
                        # Next item is not a token
                        is_token = False
        return ret_str
