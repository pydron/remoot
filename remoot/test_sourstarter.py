# Copyright (C) 2015 Stefan C. Mueller
import unittest
import logging
import sys
from pickle import PickleError
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)


import utwist
from twisted.internet import defer

import sourblossom

import test_credentials
from remoot import sourstarter, pythonstarter

class SmartStarterTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        test_credentials.load_credentials()
    
    @defer.inlineCallbacks
    def twisted_setup(self):
        starter = pythonstarter.LocalStarter()
#         starter = pythonstarter.SSHStarter(test_credentials.ssh_hostname, 
#                            test_credentials.ssh_user, 
#                            private_key_files=[test_credentials.ssh_privatekey],
#                            tmp_dir=test_credentials.ssh_tmp_dir)
        
        self.myaddr = yield sourblossom.listen(("localhost", 4000))
        self.target = sourstarter.SmartStarter(starter, ("localhost", 4001))
        
    @defer.inlineCallbacks
    def twisted_teardown(self):
        yield sourblossom.shutdown()

    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_start_stop(self):
        process = yield self.target.start()
    
        yield process.reset()

        yield process.stop()


    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_call(self):
        process = yield self.target.start()
        yield process.reset()
        func = yield process.register(say_hello)
        actual = yield func()
        self.assertEquals("Hello", actual)
        yield process.stop()
        

#     @utwist.with_reactor
#     @defer.inlineCallbacks
#     def test_latency(self):
#         process = yield self.target.start()
#         yield process.reset()
# 
#         func = yield process.register(say_hello)
#         start = time.time()
#         for i in range(100):
#             actual = yield func()
#         duration = time.time() - start
#         self.assertEquals("Hello", duration)
#         yield process.stop()
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_pickle_failure(self):
        process = yield self.target.start()
    
        yield process.reset()
        
        func = yield process.register(get_not_pickleable)
        
        try:
            logger.debug("calling")
            try:
                yield func()
            finally:
                logger.debug("finished")
            raise AssertionError("Expected PickleError")
        except PickleError:
            pass # expected
        
        yield process.stop()
        

def get_not_pickleable():
    return type(None)

def say_hello():
    return "Hello"