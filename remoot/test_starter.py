# Copyright (C) 2014 Stefan C. Mueller

import sys
import unittest2 as unittest
import uuid
import os.path
import tempfile
from utwist import with_reactor

import starter
import test_credentials


class StarterMixin(object):
    
    #: List of strings for the command that starts a python interpreter.
    #: Set this in subclasses.
    PYTHON_INTERPRETER = None
    TMP_DIR = None
    
    @classmethod
    def setUpClass(cls):
        test_credentials.load_credentials()
    
    @with_reactor
    def test_start(self):

        def exited(data):
            self.assertEqual("hello", data)
        
        d = self.target.start(self.PYTHON_INTERPRETER + ["-c", "import sys;sys.stdout.write('hello')"])
        d.addCallback(started)
        d.addCallback(exited)
        return d
    
    @with_reactor
    def test_start_exitcode_zero(self):
        
        def started(process):
            return process.exited.next_event()

        def exited(reason):
            self.assertIsNone(reason)
        
        d = self.target.start(self.PYTHON_INTERPRETER + ["-c", "import sys;sys.stdout.write('hello')"])
        d.addCallback(started)
        d.addCallback(exited)
        return d
    
    @with_reactor
    def test_start_exitcode_32(self):
        
        def started(process):
            return process.exited.next_event()

        def success(_):
            self.fail("Expected execution to fail with non-zero exit code.")

        def fail(reason):
            self.assertEqual(32, reason.value.exitCode)
        
        d = self.target.start(self.PYTHON_INTERPRETER + ["-c", "exit(32)"])
        d.addCallback(started)
        d.addCallbacks(success, fail)
        return d
    
    
    @with_reactor
    def test_kill(self):
        
        def started(proc):
            data = []
            proc.stdout.add_callback(data.append)
            proc.stderr.add_callback(sys.stderr.write)
            d = proc.kill()
            d.addCallback(lambda _:"".join(data))
            return d

        def exited(data):
            self.assertNotIn("not killed", data)
        
        d = self.target.start(self.PYTHON_INTERPRETER + ["-c", "import sys;import time;time.sleep(5);sys.stdout.write('not killed')"])
        d.addCallback(started)
        d.addCallback(exited)
        return d
    


    @with_reactor
    def test_start_with_file(self):
        
        def exited(data):
            self.assertEqual(fileset[filename], data)
        
        filename = str(uuid.uuid4())
        
        fileset = {filename:  str(uuid.uuid4())}
        
        d = self.target.start(self.PYTHON_INTERPRETER + ["-c", "import sys;sys.stdout.write(open('%s').read())" % filename], 
                              fileset=fileset)
        d.addCallback(started)
        d.addCallback(exited)
        return d
        
    @with_reactor
    def test_working_directory(self):
        
        def exited(data):
            self.assertEqual(self.TMP_DIR, data)
        
        filename = str(uuid.uuid4())
        
        fileset = {filename:  str(uuid.uuid4())}
        
        d = self.target.start(self.PYTHON_INTERPRETER + ["-c", "import sys,os;sys.stdout.write(os.getcwd())"], 
                              fileset=fileset)
        d.addCallback(started)
        d.addCallback(exited)
        return d
        

class TestSSHStarter(unittest.TestCase, StarterMixin):
    
    PYTHON_INTERPRETER = ["/usr/bin/env", "python"]
    TMP_DIR = test_credentials.ssh_tmp_dir
    
    @classmethod
    def setUpClass(cls):
        test_credentials.load_credentials()
        cls.TMP_DIR = test_credentials.ssh_tmp_dir
        
    def setUp(self):
        self.target = starter.SSHStarter(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey],
                           tmp_dir=test_credentials.ssh_tmp_dir)
        

        

class TestLocalStarter(unittest.TestCase, StarterMixin):
    
    PYTHON_INTERPRETER = [sys.executable]
    TMP_DIR = os.path.realpath(tempfile.gettempdir())
    
    def setUp(self):
        self.target = starter.LocalStarter(tmp_dir=self.TMP_DIR)
        
@unittest.skip
class TestEC2Starter(unittest.TestCase, StarterMixin):
    
    PYTHON_INTERPRETER = ["/usr/bin/env", "python"]
    TMP_DIR = test_credentials.ec2_tmp_dir
    
    def setUp(self):
        
        self.target = starter.EC2Starter(username = test_credentials.ec2_username,
                                         provider = test_credentials.ec2_provider, 
                                         provider_keyid = test_credentials.ec2_accesskeyid,
                                         provider_key = test_credentials.ec2_accesskey,
                                         image_id = test_credentials.ec2_imageid, 
                                         size_id = test_credentials.ec2_sizeid, 
                                         public_key_file = test_credentials.ec2_publickey, 
                                         private_key_file = test_credentials.ec2_privatekey, 
                                         tmp_dir = test_credentials.ec2_tmp_dir)
        

def started(proc):
    data = []
    proc.stdout.add_callback(data.append)
    proc.stderr.add_callback(sys.stderr.write)
    d = proc.exited.next_event()
    d.addCallback(lambda _:"".join(data))
    return d