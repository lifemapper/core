# coding=utf-8
"""
@summary: Module containing unit tests for the LmCommon.common.lmXml module
@author: CJ Grady
@version: 1.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import logging
from types import IntType
import unittest

from LmCommon.common.lmAttObject import LmAttObj
from LmCommon.common.lmXml import CDATA, deserialize, Element, QName, \
                                  setDefaultNamespace, serialize, SubElement, \
                                  tostring, tostringlist

# .............................................................................
# Module-wide list of tested items from LmCommon.common.lmXml
# As tests are written for each import, the import should be added to this list
testedItems = ['CDATA', 'deserialize', 'Element', 'serialize', 
               'setDefaultNamespace', 'SubElement', 'tostring', 'tostringlist']

# .............................................................................
class TestImportTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all imports are tested
   """
   # ...................................
   def test_coverage(self):
      """
      @summary: This function attempts to determine if all of the imports 
                   available in lmXml have been tested.  It does this by
                   looking at the testedItems list and seeing if the import is 
                   included.  If there are any imports that exist in the 
                   LmCommon.common.lmXml module that have not been included
                   in the testedItems list, this will throw an error.  
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      # Items to skip, includes direct ElementTree imports
      skipItems = ['Comment', 'ElementPath', 'ElementTree', 'HTML_EMPTY', 'PI', 
               'ParseError', 'ProcessingInstruction', 'QName', 'TreeBuilder', 
               'XML', 'XMLID', 'XMLParser', 'XMLTreeBuilder', 'dump', 
               'fromstring', 'fromstringlist', 'iselement', 'iterparse', 
               'parse', 'register_namespace', 'VERSION',
               # Other imports to skip
               'BuiltinFunctionType', 'BuiltinMethodType', 'IntType', 
               'FloatType', 'FunctionType', 'LambdaType', 'ListType', 
               'MethodType', 'NoneType', 'StringType', 'TypeType', 'DictType', 
               'UnicodeType', 'ET', 'LmAttList', 'LmAttObj', 'toUnicode', 're', 
               'sys', 'warnings'
               ]
      
      import LmCommon.common.lmXml as allImports
      for item in dir(allImports):
         if not item.startswith('_') and \
              item not in skipItems:
            self.assertIn(item, testedItems)

# .............................................................................
class TestElementTreeWrappers(unittest.TestCase):
   """
   @summary: Test class for Lifemapper wrappers for ElementTree functions and 
                classes
   """
   # ...................................
   def test_element(self):
      """
      @summary: Tests for Element
      """
      el1 = Element('testElement')
      self.assertEqual(tostring(el1), '<testElement />')
      
      el2 = Element('someEl', attrib={'att1' : 'val1'})
      self.assertIn('att1="val1"', tostring(el2))
      
      el3 = Element('someOtherEl', value='some value')
      self.assertIn('some value', tostring(el3))

   # ...................................
   def test_sub_element(self):
      """
      @summary: Test the SubElement function
      """
      el = Element('topElement')
      SubElement(el, 'firstSE')
      SubElement(el, 'secondSE', value='some value')
      SubElement(SubElement(el, 'thirdSE', attrib={'att1': 'val1'}), 'fourthSE')
      SubElement(el, QName('http://lifemapper.org', 'fifthSE'), value='another value')
      elStr = tostring(el)
      self.assertIn('firstSE', elStr)
      self.assertIn('secondSE', elStr)
      self.assertIn('thirdSE', elStr)
      self.assertIn('fourthSE', elStr)
      self.assertIn('fifthSE', elStr)
      self.assertLess(elStr.find('thirdSE'), elStr.find('fourthSE'))
      
   # ...................................
   def test_to_string(self):
      """
      @summary: Tests the tostring function
      """
      uString = u'ďáĵįÀhcç'
      uString2 = u'ᶼḠṐᵆ'
      el = Element('test')
      SubElement(el, 'someEl', value=uString)
      SubElement(el, 'anotherEl', attrib={'att1' : 'val1', 'att2' : uString2})
      strVal = tostring(el)
      
      self.assertIn(uString.encode('ascii', 'xmlcharrefreplace'), strVal)
      self.assertIn(uString2.encode('ascii', 'xmlcharrefreplace'), strVal)
      self.assertIn('att1', strVal)
      self.assertIn('val1', strVal)
      self.assertLess(strVal.find('att1'), strVal.find('val1'))
      
   # ...................................
   def test_to_string_list(self):
      """
      @summary: Tests the tostringlist function
      """
      uString = u'ďáĵįÀhcç'
      uString2 = u'ᶼḠṐᵆ'
      el = Element('test')
      SubElement(el, 'someEl', value=uString)
      SubElement(el, 'anotherEl', attrib={'att1' : 'val1', 'att2' : uString2})
      strVals = tostringlist(el)
      
      self.assertTrue(any([uString.encode('ascii', 'xmlcharrefreplace') in val for val in strVals]))
      self.assertTrue(any([uString2.encode('ascii', 'xmlcharrefreplace') in val for val in strVals]))
      self.assertTrue(any(['att1' in val for val in strVals]))
      self.assertTrue(any(['val1' in val for val in strVals]))
   
# .............................................................................
class TestLifemapperAddons(unittest.TestCase):
   """
   @summary: Test class for Lifemapper add-ons to ElementTree
   """
   # ...................................
   def test_cdata(self):
      """
      @summary: Tests the CDATA method to ensure that a proper CDATA element is
                   inserted into the tree
      """
      el = Element('test')
      el.append(CDATA('this text should be inside of a CDATA tag'))
      res = tostring(el)
      self.assertIn('<![CDATA[', res)
      self.assertIn(']]>', res)
      self.assertLess(res.find('<![CDATA['), res.find(']]>'))

   # ...................................
   def test_deserialize_keep_namespace(self):
      """
      @summary: Tests the deserialize function
      """
      el = Element('SomeElement')
      SubElement(el, 'someSubElement', value='testVal')
      SubElement(el, QName('http://lifemapper.org', 'someOtherElement'), 
                  attrib={'att1' : 'val1'})
      testObj = deserialize(el, removeNS=False)
      
      self.assertEqual(testObj.someSubElement, 'testVal')
      
   # ...................................
   def test_deserialize_no_namespace(self):
      """
      @summary: Tests the deserialize function
      """
      el = Element('SomeElement')
      SubElement(el, 'someSubElement', value='testVal')
      SubElement(el, QName('http://lifemapper.org', 'someOtherElement'), 
                  attrib={'att1' : 'val1'}, value='testing')
      SubElement(el, 'other')
      SubElement(el, 'empty text', value="   ")
      SubElement(SubElement(el, 'foo'), 'bar', value='zzz')
      SubElement(el, 'tag1', value=1)
      SubElement(el, 'tag1', value=2)
      SubElement(el, 'tag1', value=3)
      SubElement(el, 'tag1', value=4)
      SubElement(el, 'tag1', value=5)
      le = SubElement(el, 'tag2s')
      SubElement(le, 'tag2', value='a')
      SubElement(le, 'tag2', value='b')
      SubElement(le, 'tag2', value='c')
      SubElement(le, 'tag2', value='d')
      SubElement(le, 'tag2', value='e')
      
      testObj = deserialize(el)
      
      self.assertEqual(testObj.someSubElement, 'testVal')
      
   # ...................................
   def test_serialize(self):
      """
      @summary: Tests the serialize function
      """
      class A(object):
         def __init__(self, someValue):
            self.something = someValue
      a = A('test')
      a.b = {'da1' : 'dv1', 'aaa' : 3, 'bbb' : None, 
             'ddd' : LmAttObj(attrib={'a' : 1, 'b' : 'apple', 'c' : [1, 2, 3], 'd' : None}, 
                              name='test2')}
      a.c = None
      a.d = [1, 2, 3]
      self.assertIsInstance(serialize(a), Element)
      
      b = LmAttObj(attrib={'a' : 1, 'banana' : 'fruit'}, name='test')
      self.assertIsInstance(serialize(b), Element)
      
      c = ['a', None, 1]
      self.assertIsInstance(serialize(c), Element)
      
      self.assertIsInstance(serialize(IntType), Element)
      
      d = LmAttObj()
      d.attrib = {'a' : 1, 'b' : None, 'c' : 'asdfasd'}
      self.assertIsInstance(serialize(d), Element)
      
      e = LmAttObj()
      e.value = 'test'
      self.assertIsInstance(serialize(e), Element)
      
   # ...................................
   def test_set_default_namespace(self):
      """
      @summary: Tests that the setDefaultNamespace function correctly sets the 
                   default namespace
      """
      el1 = Element('firstElement')
      tag1 = el1.tag
      self.assertIsInstance(tag1, QName)
      self.assertFalse(tag1.text.startswith('{'))
      
      testNamespace1 = "http://test.com"
      testNamespace2 = "http://somethingelse.com"
      
      setDefaultNamespace(testNamespace1)
      el2 = Element('secondElement')
      tag2 = el2.tag
      self.assertIsInstance(tag2, QName)
      self.assertTrue(tag2.text.startswith("{%s}" % testNamespace1))
      
      # Test that setting the default namespace a second time really changes it
      setDefaultNamespace(testNamespace2)
      el3 = Element('thirdElement')
      tag3 = el3.tag
      self.assertIsInstance(tag3, QName)
      self.assertTrue(tag3.text.startswith("{%s}" % testNamespace2))
      
      # Test if the default namespace can be set back to None
      setDefaultNamespace(None)
      el4 = Element('fourthElement')
      tag4 = el4.tag
      self.assertIsInstance(tag4, QName)
      self.assertFalse(tag4.text.startswith('{'))

# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.lmXml module
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestImportTestCoverage))
   testSuites.append(loader.loadTestsFromTestCase(TestElementTreeWrappers))
   testSuites.append(loader.loadTestsFromTestCase(TestLifemapperAddons))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
