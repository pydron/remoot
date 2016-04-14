# Copyright (C) 2015 Stefan C. Mueller
import unittest
import logging
import sys
from pickle import PickleError


logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)


import utwist
from twisted.internet import defer

import anycall


from remoot import smartstarter, pythonstarter

class SmartStarterTest(unittest.TestCase):
    
    @defer.inlineCallbacks
    def twisted_setup(self):
        starter = pythonstarter.LocalStarter()
        self.rpc = anycall.create_tcp_rpc_system()
        
        yield self.rpc.open()
        
        self.target = smartstarter.SmartStarter(starter, self.rpc, anycall.create_tcp_rpc_system, [], preconnect_peer="localhost:8005", factory_kwargs={"port":8005})
        
    @defer.inlineCallbacks
    def twisted_teardown(self):
        yield self.rpc.close()

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
        
        url = yield process.get_function_url(say_hello)
        func = self.rpc.create_function_stub(url)
        
        actual = yield func()
        self.assertEquals("Hello", actual)

        yield process.stop()
        
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_pickle_failure(self):
        process = yield self.target.start()
    
        yield process.reset()
        
        url = yield process.get_function_url(get_not_pickleable)
        func = self.rpc.create_function_stub(url)
        
        try:
            yield func()
            raise AssertionError("Expected PickleError")
        except PickleError:
            pass # epxected
        
        yield process.stop()
        
    @utwist.with_reactor
    @defer.inlineCallbacks
    def test_default_rpcsystem(self):
        process = yield self.target.start()
    
        yield process.reset()
        
        url = yield process.get_function_url(get_rpcname)
        func = self.rpc.create_function_stub(url)
        
        actual = yield func()
        self.assertEqual(repr(anycall.RPCSystem), actual)
        
        yield process.stop()

def get_rpcname():
    return repr(anycall.RPCSystem.default.__class__)

def get_not_pickleable():
    return type(None)

def say_hello():
    return "Hello"