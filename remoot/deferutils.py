# Copyright (C) 2014 Stefan C. Mueller

from twisted.internet import defer
import sourblossom
import twistit

class Event(object):
    
    def __init__(self):
        self._callbacks = []
    
    def add_callback(self, callback):
        self._callbacks.append(callback)
    
    def remove_callback(self, callback):
        self._callbacks.remove(callback)
    
    def fire(self, value):
        for callback in self._callbacks:
            callback(value)
    
    def derive(self, modifier):
        def forward(value):
            changed_value = modifier(value)
            derived.fire(changed_value)
        
        derived = Event()
        self.add_callback(forward)
        return derived
    
    def next_event(self):
        """
        Returns a :class:`~defer.Deferred` that will be called back
        with the value of the next event.
        """
        d = defer.Deferred()
        self.add_callback(d.callback)
        return d
    
    def make_stub(self, rpcsystem):
        add_callback = rpcsystem.create_local_function_stub(self.add_callback)
        remove_callback = rpcsystem.create_local_function_stub(self.remove_callback)
        fire = rpcsystem.create_local_function_stub(self.fire)
        next_event = rpcsystem.create_local_function_stub(self.next_event)
        
        stub = EventStub(rpcsystem.ownid)
        stub.add_callback = add_callback
        stub.remove_callback = remove_callback
        stub.fire = fire
        stub.next_event = next_event
        return stub
    
    @twistit.yieldefer
    def make_sour_stub(self):
        add_callback = yield sourblossom.register(self.add_callback)
        remove_callback = yield sourblossom.register(self.remove_callback)
        fire = yield sourblossom.register(self.fire)
        next_event = yield sourblossom.register(self.next_event)
        
        stub = EventStub(None)
        stub.add_callback = add_callback
        stub.remove_callback = remove_callback
        stub.fire = fire
        stub.next_event = next_event
        defer.returnValue(stub)
    
class EventStub(object):
    def __init__(self, ownid):
        self.ownid = ownid
        
    def __repr__(self):
        return "EventStub(%s)" % repr(self.ownid)
        
    def make_sour_stub(self):
        return self
    
    def make_stub(self, rpcsystem):
        assert self.ownid == rpcsystem.ownid
        return self
