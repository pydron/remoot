# Copyright (C) 2014 Stefan C. Mueller

import unittest
import StringIO as stringio
import tempfile
import sys

import remoot
import zipfile
import ziploader

class TestZipLoader(unittest.TestCase):
    
    def test_contains_file(self):
        zf = zipfile.ZipFile(stringio.StringIO(ziploader.make_package_zip([remoot])))
        self.assertTrue("remoot/ziploader.py" in zf.namelist())
        
    def test_import(self):
        f = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        filename = f.name
        
        zf = zipfile.ZipFile(f, 'w')
        zf.writestr("foobar/testmodule.py", "MESSAGE='Hello World!'")
        zf.writestr("foobar/__init__.py", "")
        zf.close()
        f.close()
        
        ziploader.register_zip(filename)
        
        __import__("foobar.testmodule")
        actual = sys.modules["foobar.testmodule"].MESSAGE
        self.assertEqual(actual, "Hello World!")