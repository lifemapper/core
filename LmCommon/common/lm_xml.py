"""Module containing Lifemapper XML utilities

Note: Mainly wraps elementTree functionality to fit Lifemapper needs
"""
from types import (BuiltinFunctionType, BuiltinMethodType, FunctionType,
                   LambdaType, MethodType)

from LmCommon.common.attribute_object import LmAttList, LmAttObj
import xml.etree.ElementTree as ET

# Functions / Classes directly mapped to the Element Tree versions
# ..............................................................................
Comment = ET.Comment
ElementPath = ET.ElementPath
ElementTree = ET.ElementTree
HTML_EMPTY = ET.HTML_EMPTY
PI = ET.PI
ParseError = ET.ParseError
ProcessingInstruction = ET.ProcessingInstruction
QName = ET.QName
TreeBuilder = ET.TreeBuilder
VERSION = ET.VERSION
XML = ET.XML
XMLID = ET.XMLID
XMLParser = ET.XMLParser
#XMLTreeBuilder = ET.XMLTreeBuilder
dump = ET.dump
fromstring = ET.fromstring
fromstringlist = ET.fromstringlist
iselement = ET.iselement
iterparse = ET.iterparse
parse = ET.parse
re = ET.re
register_namespace = ET.register_namespace
sys = ET.sys
warnings = ET.warnings

# Functions / classes modified to serve Lifemapper purposes
# global __DEFAULT_NAMESPACE__
__DEFAULT_NAMESPACE__ = None


# .............................................................................
def set_default_namespace(def_namespace):
    """Set the default namespace.

    Args:
        defNamespace: The default namespace to use.
    """
    # Need to specify that we are setting the global variable
    global __DEFAULT_NAMESPACE__
    __DEFAULT_NAMESPACE__ = def_namespace


# .............................................................................
class Element(ET.Element):
    """Wrapper around ElementTree Element class.
    """

    # .............................
    def __init__(self, tag, attrib=None, value=None, namespace=-1, **extra):
        """Element constructor.

        Args:
            tag (str): The tag for the element (string or QName)
            attrib (dict): A dictionary of element attributes.
            value: The value for this element.
            namespace: The namespace of this element.
            extra: Extra named parameters that will be added to the element's
                attributes.

        Note:
            * Providing None for the namespace will result in a QName without a
                namespace.
            * Providing -1 for the namespace will result in the default
                namespace (__DEFAULT_NAMESPACE__) being used.
        """
        element_qname = _get_element_qname(namespace, tag)

        if attrib is None:
            attrib = {}
        ET.Element.__init__(
            self, element_qname, attrib=attrib, **extra)

        if value is not None:
            self.text = value


# .............................................................................
def CDATA(text=None):
    """Adds the capability to add CDATA elements to the XML.
    """
    element = Element('![CDATA[', namespace=None)
    element.text = text
    return element


# .............................................................................
def SubElement(parent, tag, attrib=None, value=None, namespace=-1, **extra):
    """SubElement constructor.

    Args:
        parent (Element): The parent of this new subelement.
        tag (str): The tag for the element (string or QName)
        attrib (dict): A dictionary of element attributes.
        value: The value for this element.
        namespace: The namespace of this element.
        extra: Extra named parameters that will be added to the element's
            attributes.

    Note:
        * Providing None for the namespace will result in a QName without a
            namespace.
        * Providing -1 for the namespace will result in the default
            namespace (__DEFAULT_NAMESPACE__) being used.
    """
    element_qname = _get_element_qname(namespace, tag)
    if attrib is None:
        attrib = {}
    sub_element = ET.SubElement(
        parent, element_qname, attrib=attrib, **extra)
    if value is not None:
        sub_element.text = value
    return sub_element


# .............................................................................
def tostring(element, encoding=None, method=None):
    """ElementTree.tostring wrapper that pretty prints the tree
    """
    _pretty_format(element, level=0)
    return ET.tostring(element, encoding=encoding, method=method)


# .............................................................................
def tostringlist(element, encoding=None, method=None):
    """ElementTree.tostringlist wrapper that pretty prints a list of strings
    """
    _pretty_format(element, level=0)
    return ET.tostringlist(element, encoding=encoding, method=method)

# =============================================================================
# =                          Helper Functions                                 =
# =============================================================================


# .............................................................................
# Monkey patch to add support for CDATA
ET._original_serialize_xml = ET._serialize_xml


# .............................................................................
def _serialize_xml(write, elem, qnames, namespaces,
                   short_empty_elements, **kwargs):
    """Monkey patch to add support for CDATA in serialization
    """
    if elem.tag == '![CDATA[':
        write('<{}{}]]>{}'.format(elem.tag, elem.txt, elem.tail))
        return None
    return ET._original_serialize_xml(
        write, elem, qnames, namespaces, short_empty_elements, **kwargs)


# .............................................................................
ET._serialize_xml = ET._serialize['xml'] = _serialize_xml


# .............................................................................
def _get_element_qname(namespace, tag):
    """Assembles a QName object from a namespace and tag

    Args:
        namespace: The namespace to use for the QName.
        tag: The tag of the QName.

    Note:
        * If namespace is -1, the default namespace (specified by
            __DEFAULT_NAMESPACE__) will be used.
        * If namespace is None, the QName will not have a namespace.
        * If tag is already a QName, it will be returned unmodified (happens
            for SubElements).
    """
    if isinstance(tag, QName):
        # If the tag is already a QName, no need to recreate it
        return tag

    if namespace == -1:
        namespace = __DEFAULT_NAMESPACE__
    if namespace is not None:
        elem_name = QName(namespace, tag)
    else:
        elem_name = QName(tag)
    return elem_name


# .............................................................................
def _pretty_format(elem, level=0):
    """Formats ElementTree element so that it prints pretty (recursive)

    Args:
        elem: ElementTree element to be pretty formatted
        level: How many levels deep to indent for
    """
    tab = "    "

    i = "\n" + level * tab
    if len(elem) > 0:
        if not elem.text or not elem.text.strip():
            elem.text = i + tab
        e_1 = None

        for my_el in elem:
            e_1 = my_el
            _pretty_format(e_1, level + 1)
            if not e_1.tail or not e_1.tail.strip():
                e_1.tail = i + tab
        if not e_1.tail or not e_1.tail.strip():
            e_1.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


# .............................................................................
def _remove_namespace_func(tag):
    """Remove the namespace from an element.

    Args:
        tag (str or QName): The tag to remove the namespace from.
    """
    if isinstance(tag, QName):
        tag = tag.text

    if tag.find('}') > 0:
        return tag.split('}')[1]
    return tag


# .............................................................................
def _dont_remove_namespace_func(tag):
    """Do not remove namespace from an element.

    Args:
        tag (str or QName): The tag to return text for.
    """
    if isinstance(tag, QName):
        return tag.text
    return tag


# =============================================================================
# =                   Object Deserialization / Serialization                  =
# =============================================================================
# .............................................................................
def deserialize(element, remove_namespace=True):
    """Deserializes an Element into an object

    Args:
        element (Element): The element to deserialize.
        remove_namespace (bool): Indicates if the namespace should be removed
            from the element tags.

    Returns:
        LmAttObj - An object representing the deserialized version of the xml.
    """
    # If remove_namespace is set to true, look for namespaces in the tag and
    #    remove them
    if remove_namespace:
        process_tag = _remove_namespace_func
    else:
        process_tag = _dont_remove_namespace_func

    # If the element has no children, just get the text
    if len(list(element)) == 0 and len(list(element.attrib.keys())) == 0:
        try:
            val = element.text.strip()
            if len(val) > 0:
                return val
            return None
        except AttributeError:
            return None
    else:
        attribs = {
            process_tag(key): element.attrib[key]
            for key in element.attrib.keys()}
        obj = LmAttObj(attrib=attribs, name=process_tag(element.tag))

        try:
            val = element.text.strip()
            if len(val) > 0:
                obj.value = val
        except AttributeError:
            pass

        # Get a list of all of the element's children's tags
        # If they are all the same type and match the parent, make one list
        tags = [child.tag for child in list(element)]
        reduced_tags = list(set(tags))

        try:
            first_reduced_tag = element.tag.text[:-1]
        except AttributeError:
            first_reduced_tag = element.tag[:-1]

        if len(reduced_tags) == 1 and reduced_tags[0] == first_reduced_tag:
            obj = LmAttList([], attrib=attribs, name=process_tag(element.tag))
            for child in list(element):
                obj.append(deserialize(child, remove_namespace))
        else:
            # Process the children
            for child in list(element):
                if hasattr(obj, process_tag(child.tag)):
                    tmp = obj.__getattribute__(process_tag(child.tag))
                    if isinstance(tmp, list):
                        tmp.append(deserialize(child, remove_namespace))
                    else:
                        tmp = LmAttList(
                            [tmp, deserialize(child, remove_namespace)],
                            name=process_tag(child.tag) + 's')
                    setattr(obj, process_tag(child.tag), tmp)
                else:
                    setattr(
                        obj, process_tag(child.tag),
                        deserialize(child, remove_namespace))
        return obj


# .............................................................................
def _attribute_filter(attribute):
    """Attribute filter function.

    Args:
        attribute (str): The name of an object attribute.

    Return:
        bool - Indicator if the attribute should be processed.
    """
    return attribute.startswith('_') and attribute not in ['attrib', 'value']


# .............................................................................
def serialize(obj, parent=None):
    """Serialize an object into XML.

    Args:
        obj (LmAttObj): The object to serialize.
        parent (Element): A parent element to attach this object to.

    Note:
        * Recursive

    Returns:
        Element - An ElementTree Element representing the object.
    """
    value = None
    attrib = {}
    if hasattr(obj, 'value'):
        value = obj.value
    elif isinstance(obj, str):
        value = obj

    obj_attribs = [att for att in dir(obj) if _attribute_filter(att)]

    if hasattr(obj, 'attrib'):
        for k, val in [
                (key, obj.attrib[key]) for key in list(obj.attrib.keys())]:
            attrib[k] = val
    try:
        atts = obj.get_attributes()
        # Filter these out of the dir determined attributes (duplicated and
        # these shouldn't be tags)
        obj_attribs = [a for a in obj_attribs if a not in atts]
        for key in list(atts.keys()):
            if isinstance(atts[key], (float, int, str)):
                attrib[key] = str(atts[key])
            elif atts[key] is None:
                pass
            else:
                obj_attribs.append(key)
    except Exception:
        pass

    if isinstance(obj, LmAttObj):
        element_name = obj.__name__
    else:
        element_name = obj.__class__.__name__

    if isinstance(obj, Element):
        if parent is None:
            elem = obj
        else:
            parent.append(obj)
            elem = obj
    else:
        if parent is None:
            elem = Element(element_name, value=value, attrib=attrib)
        else:
            elem = SubElement(parent, element_name, value=value, attrib=attrib)

        for att in obj_attribs:
            sub_obj = getattr(obj, att)
            if isinstance(sub_obj, list):
                for i in sub_obj:
                    serialize(i, elem)
            elif isinstance(
                    sub_obj, (MethodType, FunctionType, LambdaType,
                              BuiltinMethodType, BuiltinFunctionType)):
                pass
            elif isinstance(sub_obj, (float, int, str)):
                SubElement(elem, att, value=sub_obj)
            elif isinstance(sub_obj, dict):
                sub_el = SubElement(elem, att)
                for key in list(sub_obj.keys()):
                    if isinstance(sub_obj[key], (float, int, str)):
                        SubElement(sub_el, key, value=sub_obj[key])
                    elif sub_obj[key] is None:
                        pass
                    else:
                        serialize(sub_obj[key], sub_el)
            else:
                serialize(getattr(obj, att), elem)
        if isinstance(obj, list):
            for i in obj:
                serialize(i, elem)
    return elem
