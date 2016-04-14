# Copyright (C) 2014 Stefan C. Mueller

import types
import logging
import sourblossom
import twistit
import utwist
import pickle
import base64
import random

logger = logging.getLogger(__name__)

from twisted.internet import reactor, defer

import ziploader

"""
The smart starter combines :mod:`starter` with the `sourblossom` RPC system.

There are several ways to help prevent created processes to survive longer
than they should. This is an important especially because significant and costly
resources might be allocated.

 * :meth:`SmartStarter.start` returns a deferred that can be cancelled.
   Cancelling will do its best to free any resources already allocated.
   
 * :meth:`Process.stop` Will ask the process to properly shut-down and
   release any resources. This will produce a failure if not suceessful.
   
 * :meth:`Process.kill` Attemps to free the resources without help of the
   process. This should help if the process hangs, for example. Returns
   a deferred that calls back once the resources are freed and, hopefully,
   the process has terminated.
   
 * Each process has a timer that, once elapsed, causes the process to
   exit and release the resources. The master of the process has to
   call :meth:`Process.reset_watchdog` at regular intervals to avoid this
   from happening. 
   
The first three try to cover the case where the master wants a child to
end, even if the child does not react anymore. The last one covers the
case where the child is working fine, but has lost contact the master.

.. py:class:: Process

    Objects representing processes have the following attributes and methods:
    
    .. py:method:: register(function)
       
       Invokes `sourblossom.register(function)` in the remote process.
       
    .. py:method:: reset()
    
       Resets the watch-dog timer of the remote process, avoiding that it terminates
       itself because it thinks it is no longer used.
        
    .. py:method:: stop()
    
       Attempts to gracefully terminate the remote process.
      
    .. py:method:: kill()
    
        Attempts to forcefully end the process. Returns a deferred which
        calls back once the local resources are freed and the process
        (hopefully) has terminated.
        
    .. py:method:: send_stdin(data)
   
        Attempts to send data to STDIN of the process. Not all implementations
        support this.
   
        :param str data: Data to send.
        
    .. py:attribute:: stdout
    
        :class:`~remoot.deferutils.Event` fired when the process produced
        standard output data. Not all implementations will be able
        to capture this data.
        
    .. py:attribute:: stderr
    
        :class:`~remoot.deferutils.Event` fired when the process produced
        standard error data. Not all implementations will be able
        to capture this data.
        
    .. py:attribute:: exited
    
        :class:`~remoot.deferutils.Event` fired when the process has ended
          with a :class:`Failure` indiciating the reason, or `None` if
          the process has exited normally. Not all implementations can
          reliably provide this event.
"""





class SmartStarter(object):

    def __init__(self, pythonstarter, childaddr, preloaded_packages=[]):
        self.pythonstarter = pythonstarter
        self.childaddr = childaddr
        self.preloaded_packages = list(preloaded_packages)
        self.comm_setup_timeout = 60
        self.watchdog_timeout = 10 * 60
        
        
    @twistit.yieldefer
    def start(self):
        
        def cancel_during_wait(d):
            """
            Invoked if operation was cancelled while we wait for `init_call`.
            """
            logger.warn("Forcefully killing new child process due to cancellation.")
            return process.kill()
        
        def process_exited(reason):
            """
            Invoked if the process exits while waiting for `init_call`.
            """
            logger.error("New process died while we were expecting the first call. Reason: %s" % reason)
            if reason:
                init_call_d.errback(reason)
            else:
                try:
                    if collected_stdio_output:
                        out = repr("".join(collected_stdio_output))
                        raise ValueError("Child process exited while we expected initial contact. STDIO output: %s" % out)
                    else:
                        raise ValueError("Child process exited while we expected initial contact. No STDIO output captured.")
                except:
                    init_call_d.errback()
        
        def init_call(register, reset, stop):
            """
            Invoked by the child via RPC.
            
            We inject methods into the process object that we can use to communicate with the
            new process.
            """

            def inject_function(function, obj, name):
                
                def f(self, *args, **kwargs):
                    return function(*args, **kwargs)
                f.__name__ = name
                f.func_name = name
                
                bound = types.MethodType(f, obj)
                setattr(obj, name, bound)
                       
            logger.info("Child process called back. register:%s, reset:%s, stop:%s" % 
                        (register, reset, stop))           
                        
            def stop_and_join():
                stop_event = process.exited.next_event()
                logger.debug("calling stop()")
                d = stop()
                def wait(_):
                    logger.debug("stop() returned")
                    return stop_event
                d.addCallback(wait)
                return d
            
            inject_function(register, process, "register")
            inject_function(reset, process, "reset")
            inject_function(stop_and_join, process, "stop")
            
            process.exited.remove_callback(process_exited)
            init_call_d.callback(process)
        
        uid = random.randint(0, 1000000)
        zip_filename = "code_%s.zip" % uid
        init = sourblossom.register(init_call)
        boot_script = self._make_boot_script(self.childaddr, init, zip_filename)
        zip_content = yield self._make_zip_content(self.preloaded_packages)
        
        
        collected_stdio_output = []
        
        init_call_d = defer.Deferred(cancel_during_wait)
        
        
        logger.info("Starting process with %s" % type(self.pythonstarter))
        for line in boot_script.splitlines():
            logger.debug("Boot script: %s" % line)
            
        process = yield self.pythonstarter.start(boot_script, {zip_filename:zip_content})
        
        def stdout_received(txt):
            collected_stdio_output.append(txt)
            for line in txt.splitlines():
                logger.info("stdout from child: %s" % repr(line))
        def stderr_received(txt):
            collected_stdio_output.append(txt)
            for line in txt.splitlines():
                logger.info("stderr from child: %s" % repr(line))
        
        process.stdout.add_callback(stdout_received)
        process.stderr.add_callback(stderr_received)
        process.exited.add_callback(process_exited)
        
        logger.debug("Waiting for process to call us...")
        
        # wait for the new process to call `init_call`.
        process = yield init_call_d
        
        logger.debug("Invoking reset on new process to check if communication in that direction works too")
        yield process.reset()
        
        process.stdout.remove_callback(stdout_received)
        process.stderr.remove_callback(stderr_received)
        
        logger.info("Child process started sucessfully")
        defer.returnValue(process)
        
        

    def _make_boot_script(self, myaddr, init, zip_filename):
        
        init_encoded = pickle.dumps(init, pickle.HIGHEST_PROTOCOL)
        init_encoded = base64.encodestring(init_encoded)

        return _BOOT_SCRIPT.format(
            zip_filename = repr(zip_filename),
            this_module = __name__, 
            myaddr=myaddr, 
            init_encoded=repr(init_encoded),
            kill_module = self.pythonstarter.kill.__module__,
            kill_name = self.pythonstarter.kill.__name__,
            comm_setup_timeout=self.comm_setup_timeout,
            watchdog_timeout=self.watchdog_timeout)
    
    def _make_zip_content(self, preloaded_packages):
        import remoot
        import picklesize
        packages = list(preloaded_packages) + [remoot, sourblossom, twistit, utwist, picklesize]
        return ziploader.cached_make_package_zip(packages)


@twistit.yieldefer
def _boot(myaddr, init_encoded, kill_function, comm_setup_timeout, watchdog_timeout):
    """
    Invoked by the new process.
    """
    logger.info("Starting this process...")
    
    watchdog = _Watchdog(watchdog_timeout)
    
    @twistit.yieldefer
    def shutdown():
        try:
            logger.debug("Closing RPC system...")
            yield twistit.timeout_deferred(sourblossom.shutdown(), comm_setup_timeout, 
                                           "Timeout while closing RPC System")
    
            logger.info("Telling the reactor to stop...")
            reactor.stop()  # @UndefinedVariable
                
        except:
            logger.exception("Error while initializing process.")
            
        finally:
            logger.debug("Calling kill function...")
            kill_function()
            logger.info("Process will now exit")
            
    try:
        
        logger.debug("Opening port...")
        myaddr = twistit.timeout_deferred(sourblossom.listen(myaddr), comm_setup_timeout)
        
        logger.debug("Unpickling RPC stub...")
        init_encoded = base64.decodestring(init_encoded)
        init = pickle.loads(init_encoded)
        
        logger.debug("Creating initial set of stubs...")
        register = sourblossom.register(sourblossom.register)
        reset = sourblossom.register(watchdog.reset)
        stop = sourblossom.register(watchdog.stop)
                
        logger.info("Calling `init` of parent...")
        d = init(register, reset, stop)
        yield twistit.timeout_deferred(d, comm_setup_timeout, 
               "Timeout during first attempt to communicate with parent.")
        logger.info("Process has started.")
        
        yield watchdog.start()

    except:
        logger.exception("Error while initializing process.")
    finally:
        reactor.callLater(1, shutdown)  # @UndefinedVariable
    

class _Watchdog(object):
    """
    A timer that needs to be reset at regular intervals to avoid
    a timeout error.
    """
    
    def __init__(self, timeout):
        """
        :param timeout: Seconds til we timeout if we don't receive a :meth:`reset` call.
        """
        self.timeout = timeout
        self.deferred = None
        self._delayed_call = None
        
        self._status = "not started"
        
    def start(self):
        """
        Starts the watchdog timer. Returns a deferred that will callback if
        the watchdog is stopped or errbacks if the watchdog times out.
        """
        if self._status != "not started":
            raise ValueError("Already started")
        self._status = "running"
        
        self.deferred = defer.Deferred()
        self._delayed_call = reactor.callLater(self.timeout, self._on_timeout)  # @UndefinedVariable
        
        return self.deferred
    
    def stop(self):
        """
        Gracefully stop the watchdog timer. The deferred returned from :meth:`start`
        will callback with `None`.
        
        If we aren't running, the call is ignored.
        """
        logger.debug("stopping")
        if self._status == "running":
            self._status = "stopped"
            self._delayed_call.cancel()
            self._delayed_call = None
            self.deferred.callback(None)
            self.deferred = None
    
    def reset(self):
        """
        Reset the timer. This should be called regularly to prevent the deferred
        returned by :meth:`start` to fail with a :class:`anycall.Timeout` error.
        
        If we aren't running, the call is ignored.
        """
        if self._status == "running":
            self._delayed_call.cancel()
            self._delayed_call = reactor.callLater(self.timeout, self._on_timeout)  # @UndefinedVariable
        
        
    def _on_timeout(self):
        if self._status == "running":
            self._status = "stopped"
            self._delayed_call = None
            try:
                raise twistit.TimeoutError("Watchdog timeout. No reset in %s seconds." % self.timeout)
            except:
                self.deferred.errback()
                self.deferred = None
    



#: Script passed to the started interpreter for execution. 
_BOOT_SCRIPT = """
import sys
import os.path
import traceback
import logging
import signal
import site
if hasattr(signal, "SIGHUP"):
    signal.signal(signal.SIGHUP, lambda *args:os._exit(1))
logging.basicConfig(level=logging.WARN, stream=sys.stdout)
codepath = os.path.abspath({zip_filename})
sys.path.insert(0, codepath)
reload(site)
import {kill_module}
try:
    import {this_module}
    from twisted.internet import reactor
    {this_module}._boot(
        {myaddr}, 
        {init_encoded}, 
        {kill_module}.{kill_name},
        {comm_setup_timeout},
        {watchdog_timeout})
    reactor.run()
except:
    traceback.print_exc()
    {kill_module}.{kill_name}()
    
"""

import site