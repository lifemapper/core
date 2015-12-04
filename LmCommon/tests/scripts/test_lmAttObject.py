"""
@summary: This module contains unit tests for the LmCommon.common.lmAttObject 
             module
@author: CJ Grady
@version: 1.0
@status: beta

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
import unittest

from LmCommon.common.lmAttObject import LmAttObj, LmAttList

# .............................................................................
# Module-wide list of tested items from LmCommon.common.lmAttObject
# As tests are written for each import, the import should be added to this list
testedItems = ['LmAttObj', 'LmAttList']

# .............................................................................
class TestImportTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all imports are tested
   """
   # ...................................
   def test_coverage(self):
      """
      @summary: This function attempts to determine if all of the imports 
                   available in lmAttObject have been tested.  It does this by
                   looking at the testedItems list and seeing if the import is 
                   included.  If there are any imports that exist in the 
                   LmCommon.common.lmAttObject module that have not been 
                   included in the testedItems list, this will throw an error.
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      import LmCommon.common.lmAttObject as allImports
      for item in dir(allImports):
         if not item.startswith('_'):
            self.assertIn(item, testedItems)

# .............................................................................
class TestLmAttList(unittest.TestCase):
   """
   @summary: Test class for LmAttList
   """
   # ...................................
   def test_simple_object(self):
      """
      @summary: This test attempts to create a simple object.  The expected 
                   response is for a LmAttList to be created.
      """
      self.assertIsInstance(LmAttList(), LmAttList)
   
   # ...................................
   def test_simple_object_with_name(self):
      """
      @summary: This test attempts to create a simple object with a name.  The
                   expected response is for a LmAttList to be created with the
                   correct __name__ attribute.
      """
      name = "testname"
      testObj = LmAttList(name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.__name__, name)
   
   # ...................................
   def test_create_object_with_attributes(self):
      """
      @summary: This test attempts to create a LmAttList with some attributes.  
                   The expected response is for an LmAttList to be created with 
                   the specified attributes.
      """
      name = "testname"
      testAttribs = {"att1" : "val1", "att2" : "val2"}
      testObj = LmAttList(attrib=testAttribs, name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.__name__, name)
      self.assertEqual(testAttribs, testObj._attrib)
   
   # ...................................
   def test_create_object_with_items(self):
      """
      @summary: This test attempts to create a LmAttList and then add some 
                   object attributes (public members).  The expected response 
                   is for an LmAttList to be created and the specified values to 
                   be on the object.
      """
      val1 = 'val1'
      val2 = 3
      testObj = LmAttList(name="test")
      testObj.member1 = val1
      testObj.member2 = val2
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.member1, val1)
      self.assertEqual(testObj.member2, val2)
   
   # ...................................
   def test_create_object_with_attributes_and_items(self):
      """
      @summary: This test attempts to create a LmAttList with attributes as well 
                   as member items.  The expected response is for an LmAttList 
                   to be created with the specified attributes and members.
      """
      name = "testname"
      val1 = 'val1'
      val2 = 3
      testAttribs = {"att1" : "val1", "att2" : "val2"}
      testObj = LmAttList(attrib=testAttribs, name=name)
      testObj.member1 = val1
      testObj.member2 = val2
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.member1, val1)
      self.assertEqual(testObj.member2, val2)
      self.assertEqual(testObj.__name__, name)
      self.assertEqual(testAttribs, testObj._attrib)
   
   # ...................................
   def test_getAttributes(self):
      """
      @summary: This test attempts to retrieve the attributes from an LmAttList.
                   The expected response is for the attributes retrieved to be
                   the same as the attributes used for creation.
      """
      name = "testname"
      testAttribs = {"att1" : "val1", "att2" : "val2"}
      testObj = LmAttList(attrib=testAttribs, name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.__name__, name)
      self.assertEqual(testAttribs, testObj.getAttributes())
   
   # ...................................
   def test_set_attribute(self):
      """
      @summary: This test attempts to set an attribute on a created LmAttList 
                   object.  The expected response is for the attribute to be
                   set and the value changed.
      """
      key = "test key"
      origValue = "original value"
      newValue = 3
      name = "testname"
      testObj = LmAttList(attrib={key: origValue}, name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.getAttributes()[key], origValue)
      self.assertNotEqual(testObj.getAttributes()[key], newValue)
      testObj.setAttribute(key, newValue)
      self.assertEqual(testObj.getAttributes()[key], newValue)
      self.assertNotEqual(testObj.getAttributes()[key], origValue)
   
   # ...................................
   def test_get_attribute(self):
      """
      @summary: This test attempts to retrieve an attribute by requesting an
                   object member.  The expected response is for the attribute
                   to be returned.
      """
      key = "k"
      val = "some value"
      name = "testname"
      testObj = LmAttList(attrib={key: val}, name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.k, val)
   
   # ...................................
   def test_get_set_item(self):
      """
      @summary: This test attempts to get and set a member item for an 
                   LmAttList.  The expected response is for the item to be first 
                   set successfully and then retrieved from the object.
      """
      name = "testname"
      val1 = 'val1'
      val2 = 3
      testObj = LmAttList(name=name)
      testObj.member1 = val1
      testObj.member2 = val2
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.member1, val1)
      self.assertEqual(testObj.member2, val2)
      self.assertEqual(testObj.__name__, name)
   
   # ...................................
   def test_item_retrieved_if_exists(self):
      """
      @summary: This test attempts to retrieve a member item from a LmAttList 
                   that also contains an attribute with the same name.  The 
                   expected result is for the member item to be returned and 
                   not the attribute.
      """
      name = "testname"
      key = "key"
      attribVal = "attribute value"
      memVal = 3
      testObj = LmAttList(attrib={key : attribVal}, name=name)
      testObj.key = memVal
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(testObj.key, memVal)
      self.assertNotEqual(testObj.key, attribVal)
   
   # ...................................
   def test_get_missing(self):
      """
      @summary: This test attempts to retrieve an attribute / member from an 
                   LmAttList that does not exist as either.  The expected 
                   response is for a KeyError to be raised. 
      """
      name = "testname"
      testObj = LmAttList(name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertRaises(KeyError, lambda x: x.badvalue, testObj)


   # ...................................
   def test_create_with_list_of_items(self):
      """
      @summary: This test attempts to create a LmAttList object with a list of
                   items.  The expected response is for the object to be 
                   created and contain the specified items.
      """
      name = "testname"
      items = [1, 2, 3, 'some string', False]
      testObj = LmAttList(items=items, name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(list(testObj), items)
   
   # ...................................
   def test_append(self):
      """
      @summary: This test attempts to create a LmAttList object with a list of
                   items and then append an item to it.  The expected response
                   is for the object to be created and the item to be appended
                   successfully.
      """
      name = "testname"
      items = [1, 2, 3, 'some string', False]
      appItem = 'a'
      testObj = LmAttList(items=items, name=name)
      self.assertIsInstance(testObj, LmAttList)
      self.assertEqual(list(testObj), items)
      testObj.append(appItem)
      self.assertEqual(testObj[-1], appItem)
    
# .............................................................................
class TestLmAttObj(unittest.TestCase):
   """
   @summary: Test class for LmAttObj
   """
   # ...................................
   def test_simple_object(self):
      """
      @summary: This test attempts to create a simple object.  The expected 
                   response is for a LmAttObj to be created.
      """
      self.assertIsInstance(LmAttObj(), LmAttObj)
   
   # ...................................
   def test_simple_object_with_name(self):
      """
      @summary: This test attempts to create a simple object with a name.  The
                   expected response is for a LmAttObj to be created with the
                   correct __name__ attribute.
      """
      name = "testname"
      testObj = LmAttObj(name=name)
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.__name__, name)
   
   # ...................................
   def test_create_object_with_attributes(self):
      """
      @summary: This test attempts to create a LmAttObj with some attributes.  
                   The expected response is for an LmAttObj to be created with 
                   the specified attributes.
      """
      name = "testname"
      testAttribs = {"att1" : "val1", "att2" : "val2"}
      testObj = LmAttObj(attrib=testAttribs, name=name)
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.__name__, name)
      self.assertEqual(testAttribs, testObj._attrib)
   
   # ...................................
   def test_create_object_with_items(self):
      """
      @summary: This test attempts to create a LmAttObj and then add some 
                   object attributes (public members).  The expected response 
                   is for an LmAttObj to be created and the specified values to 
                   be on the object.
      """
      val1 = 'val1'
      val2 = 3
      testObj = LmAttObj(name="test")
      testObj.member1 = val1
      testObj.member2 = val2
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.member1, val1)
      self.assertEqual(testObj.member2, val2)
   
   # ...................................
   def test_create_object_with_attributes_and_items(self):
      """
      @summary: This test attempts to create a LmAttObj with attributes as well 
                   as member items.  The expected response is for an LmAttObj 
                   to be created with the specified attributes and members.
      """
      name = "testname"
      val1 = 'val1'
      val2 = 3
      testAttribs = {"att1" : "val1", "att2" : "val2"}
      testObj = LmAttObj(attrib=testAttribs, name=name)
      testObj.member1 = val1
      testObj.member2 = val2
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.member1, val1)
      self.assertEqual(testObj.member2, val2)
      self.assertEqual(testObj.__name__, name)
      self.assertEqual(testAttribs, testObj._attrib)
   
   # ...................................
   def test_getAttributes(self):
      """
      @summary: This test attempts to retrieve the attributes from an LmAttObj.
                   The expected response is for the attributes retrieved to be
                   the same as the attributes used for creation.
      """
      name = "testname"
      testAttribs = {"att1" : "val1", "att2" : "val2"}
      testObj = LmAttObj(attrib=testAttribs, name=name)
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.__name__, name)
      self.assertEqual(testAttribs, testObj.getAttributes())
   
   # ...................................
   def test_set_attribute(self):
      """
      @summary: This test attempts to set an attribute on a created LmAttObj 
                   object.  The expected response is for the attribute to be
                   set and the value changed.
      """
      key = "test key"
      origValue = "original value"
      newValue = 3
      name = "testname"
      testObj = LmAttObj(attrib={key: origValue}, name=name)
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.getAttributes()[key], origValue)
      self.assertNotEqual(testObj.getAttributes()[key], newValue)
      testObj.setAttribute(key, newValue)
      self.assertEqual(testObj.getAttributes()[key], newValue)
      self.assertNotEqual(testObj.getAttributes()[key], origValue)
   
   # ...................................
   def test_get_attribute(self):
      """
      @summary: This test attempts to retrieve an attribute by requesting an
                   object member.  The expected response is for the attribute
                   to be returned.
      """
      key = "k"
      val = "some value"
      name = "testname"
      testObj = LmAttObj(attrib={key: val}, name=name)
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.k, val)
   
   # ...................................
   def test_get_set_item(self):
      """
      @summary: This test attempts to get and set a member item for an 
                   LmAttObj.  The expected response is for the item to be first 
                   set successfully and then retrieved from the object.
      """
      name = "testname"
      val1 = 'val1'
      val2 = 3
      testObj = LmAttObj(name=name)
      testObj.member1 = val1
      testObj.member2 = val2
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.member1, val1)
      self.assertEqual(testObj.member2, val2)
      self.assertEqual(testObj.__name__, name)
   
   # ...................................
   def test_item_retrieved_if_exists(self):
      """
      @summary: This test attempts to retrieve a member item from a LmAttObj 
                   that also contains an attribute with the same name.  The 
                   expected result is for the member item to be returned and 
                   not the attribute.
      """
      name = "testname"
      key = "key"
      attribVal = "attribute value"
      memVal = 3
      testObj = LmAttObj(attrib={key : attribVal}, name=name)
      testObj.key = memVal
      self.assertIsInstance(testObj, LmAttObj)
      self.assertEqual(testObj.key, memVal)
      self.assertNotEqual(testObj.key, attribVal)
   
   # ...................................
   def test_get_missing(self):
      """
      @summary: This test attempts to retrieve an attribute / member from an 
                   LmAttObj that does not exist as either.  The expected 
                   response is for a KeyError to be raised. 
      """
      name = "testname"
      testObj = LmAttObj(name=name)
      self.assertIsInstance(testObj, LmAttObj)
      self.assertRaises(KeyError, lambda x: x.badvalue, testObj)
      
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the 
                common.jobs.processes.sdm.omExperiment.omRequest module
   @return: A list of test suites
   """
   testSuites = []
   testSuites.append(unittest.TestLoader().loadTestsFromTestCase(TestImportTestCoverage))
   testSuites.append(unittest.TestLoader().loadTestsFromTestCase(TestLmAttList))
   testSuites.append(unittest.TestLoader().loadTestsFromTestCase(TestLmAttObj))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
