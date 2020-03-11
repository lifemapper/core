"""Module containing some base Lifemapper objects
"""


# .............................................................................
class LmAttObj:
    """Object that includes attributes.

    Note:
        <someElement att1="value1" att2="value2">
            <subEl1>1</subEl1>
            <subEl2>banana</subEl2>
        </someElement>

        translates to:

        obj.subEl1 = 1
        obj.subEl2 = 'banana'
        obj.getAttributes() = {'att1': 'value1', 'att2': 'value2'}
        obj.att1 = 'value1'
        obj.att2 = 'value2'
    """

    # ......................................
    def __init__(self, attrib=None, name='LmObj'):
        """Constructor

        Args:
            attrib (dict, optional): Dictionary of attributes to attach to the
                object.
            name (str): The name of the object (useful for serialization).
        """
        if not attrib:
            attrib = {}
        self.__name__ = name
        self._attrib = attrib

    # ......................................
    def __getattr__(self, name):
        """Called if the default getattribute method fails.

        Args:
            name (str): The name of the attribute to return.

        Returns:
            The value of the attribute.
        """
        return self._attrib[name]

    # ......................................
    def get_attributes(self):
        """Gets the dictionary of attributes attached to the object.

        Returns:
            dict - The attribute dictionary.
        """
        return self._attrib

    # ......................................
    def set_attribute(self, name, value):
        """Sets the value of an attribute in the attribute dictionary.

        Args:
            name (str): The name of the attribute to set.
            value (object): The new value of the attribute.
        """
        self._attrib[name] = value

    # ......................................
    def __dir__(self):
        """Override dir() method to pick up attributes in _attrib
        """

        def get_attrs(obj):
            if not hasattr(obj, '__dict__'):
                return []  # slots only
            if not isinstance(obj.__dict__, dict):
                raise TypeError(
                    '{}.__dict__ is not a dictionary'.format(obj.__name__))
            return list(obj.__dict__.keys())

        def dir2(obj):
            attrs = set()
            if not hasattr(obj, '__bases__'):
                # obj is an instance
                if not hasattr(obj, '__class__'):
                    # slots
                    return sorted(get_attrs(obj))
                klass = obj.__class__
                attrs.update(get_attrs(klass))
            else:
                # obj is a class
                klass = obj

            for cls in klass.__bases__:
                attrs.update(get_attrs(cls))
                attrs.update(dir2(cls))
            attrs.update(get_attrs(obj))
            try:
                attrs.update(list(obj.getAttributes().keys()))
            except Exception:
                pass
            return list(attrs)

        return dir2(self)


# .............................................................................
class LmAttList(list, LmAttObj):
    """Extension to lists that adds attributes.

    Note:
        >>> obj = LmAttList([1, 2, 3], {'id': 'attList'})
        >>> print(obj[0]) >>  1
        >>> obj.append('apple')
        >>> print(obj) >> [1, 2, 3, 'apple']
        >>> print(obj.id) >> 'attList'
    """

    def __init__(self, items=None, attrib=None, name='LmList'):
        """Constructor.

        Args:
            items (list): A list of initial values for the list.
            attrib (dict): A dictionary of attributes to attach to the list.
            name (str): The name of the object (useful for serialization).
        """
        list.__init__(self)
        if not items:
            items = []
        if not attrib:
            attrib = {}
        LmAttObj.__init__(self, attrib, name)
        for item in items:
            self.append(item)
