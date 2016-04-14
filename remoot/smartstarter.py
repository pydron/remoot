# Copyright (C) 2014 Stefan C. Mueller

import types
import logging
import anycall
import twistit
import utwist
import pickle
import base64

logger = logging.getLogger(__name__)

from twisted.internet import reactor, defer, task

import ziploader

"""
The smart starter combines :mod:`starter` with the `anycall` RPC system.

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
    
    .. py:method:: get_function_url(function)
       
       Invokes `RPCSystem.get_function_url(function)` in the remote process.
       
    .. py:method:: reset_watchdog()
    
       Resets the watch-dog timer of the rmeote process, avoiding that it terminates
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

    def __init__(self, 
                 pythonstarter, 
                 rpcsystem, 
                 rpcsystem_factory, 
                 preloaded_packages=[], 
                 preconnect = False,
                 data_port = 0):
        self.pythonstarter = pythonstarter
        self.rpcsystem = rpcsystem
        self.rpcsystem_factory = rpcsystem_factory
        self.preloaded_packages = list(preloaded_packages)
        self.comm_setup_timeout = 60
        self.watchdog_timeout = 10 * 60
        self.preconnect = preconnect
        self.data_port = data_port
        self.preconnect_attempts = 5
        
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
        
        def init_call(get_function_url_url, reset_url, stop_url):
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
                       
            logger.info("Child process called back. get_function:%s, reset:%s, stop:%s" % 
                        (get_function_url_url, reset_url, stop_url))           
            
            get_function_url = self.rpcsystem.create_function_stub(get_function_url_url)
            reset =  self.rpcsystem.create_function_stub(reset_url)
            stop =  self.rpcsystem.create_function_stub(stop_url)
            
            def stop_and_join():
                stop_event = process.exited.next_event()
                d = stop()
                def wait(_):
                    return stop_event
                d.addCallback(wait)
                return d
            
            inject_function(get_function_url, process, "get_function_url")
            inject_function(reset, process, "reset")
            inject_function(stop_and_join, process, "stop")

            process.get_function_url_stub = get_function_url

            process.exited.remove_callback(process_exited)
            init_call_d.callback(process)
        
        init_url = self.rpcsystem.get_function_url(init_call)
        boot_script = self._make_boot_script(init_url, self.rpcsystem.ownid)
        zip_content = self._make_zip_content(self.preloaded_packages)
        zip_filename = "code.zip"
        
        collected_stdio_output = []
        
        init_call_d = defer.Deferred(cancel_during_wait)
        
        
        logger.info("Starting process with %s" % type(self.pythonstarter))
        for line in boot_script.splitlines():
            logger.debug("Boot script: %s" % line)
            
        process = yield self.pythonstarter.start(boot_script, {zip_filename:zip_content})
        
        def stdout_received(txt):
            collected_stdio_output.append(txt)
            for line in txt.splitlines():
                logger.debug("stdout from child: %s" % repr(line))
        def stderr_received(txt):
            collected_stdio_output.append(txt)
            for line in txt.splitlines():
                logger.info("stderr from child: %s" % repr(line))
        
        process.stdout.add_callback(stdout_received)
        process.stderr.add_callback(stderr_received)
        process.exited.add_callback(process_exited)
        
        if self.preconnect:

            peer_host = process.hostname
            peer_port = self.data_port
            
            peer = "%s:%s" % (peer_host, peer_port)
        
            # Wait a bit. The process has to open the port
            yield task.deferLater(reactor, 0.5, lambda:None)
            
            logger.debug("Opening connection to child %s..." % peer)
            
            for i in range(self.preconnect_attempts - 1):
                try:
                    yield self.rpcsystem.pre_connect(peer)
                    break
                except:
                    logger.debug("Connection attempt %d of %d failed." % (i,self.preconnect_attempts) , exc_info=1)
            else:
                # final attempt.
                # this time we don't catch errors
                yield self.rpcsystem.pre_connect(peer)
        
            logger.debug("Connection to child is open")
        
        logger.debug("Waiting for process to call us...")
        
        # wait for the new process to call `init_call`.
        process = yield init_call_d
        
        logger.debug("Invoking reset on new process to check if communication in that direction works too")
        yield process.reset()
        
        process.stdout.remove_callback(stdout_received)
        process.stderr.remove_callback(stderr_received)
        
        logger.info("Child process started successfully")
        defer.returnValue(process)
        
        

    def _make_boot_script(self, init_url, ownid):
        
        if self.preconnect:
            expect_preconnect = ownid
        else:
            expect_preconnect = None

        factory_kwargs = {"port_range" : [self.data_port]}
        kwargs_encoded = base64.b64encode(
                pickle.dumps(factory_kwargs, pickle.HIGHEST_PROTOCOL))

        return _BOOT_SCRIPT.format(
            this_module = __name__, 
            factory_module=self.rpcsystem_factory.__module__, 
            factory_name=self.rpcsystem_factory.__name__,
            factory_kwargs = repr(kwargs_encoded),
            init_url=repr(init_url),
            kill_module = self.pythonstarter.kill.__module__,
            kill_name = self.pythonstarter.kill.__name__,
            comm_setup_timeout=self.comm_setup_timeout,
            watchdog_timeout=self.watchdog_timeout,
            expect_preconnect=repr(expect_preconnect))
    
    def _make_zip_content(self, preloaded_packages):
        import remoot
        packages = list(preloaded_packages) + [remoot, anycall, twistit, utwist]
        return ziploader.make_package_zip(packages)


@twistit.yieldefer
def _boot(rpcsystem_factory, 
          factory_kwargs_encoded, 
          init_url, 
          kill_function, 
          comm_setup_timeout,
          watchdog_timeout, 
          expect_preconnect):
    """
    Invoked by the new process.
    """
    logger.info("Starting this process...")

    watchdog = _Watchdog(watchdog_timeout)
    
    try:
        logger.debug("Decoding factory arguments...")
        factory_kwargs = pickle.loads(base64.b64decode(factory_kwargs_encoded))
    
        logger.debug("Creating RPC system...")
        rpcsystem = yield twistit.timeout_deferred(defer.maybeDeferred(rpcsystem_factory, **factory_kwargs), 
                                                   comm_setup_timeout, 
                                                   "Timeout while creating RPC system")
        anycall.RPCSystem.default = rpcsystem
        
        logger.debug("Opening RPC system...")
        yield twistit.timeout_deferred(rpcsystem.open(), comm_setup_timeout, 
                                       "Timeout while opening RPC system")
        
        logger.debug("Creating RPC stub and RPC urls...")
        init = rpcsystem.create_function_stub(init_url)
        get_function_url_url = rpcsystem.get_function_url(rpcsystem.get_function_url)
        reset_url = rpcsystem.get_function_url(watchdog.reset)
        stop_url = rpcsystem.get_function_url(watchdog.stop)
        
        if expect_preconnect is not None:
            logger.info("Waiting for parent to open a connection to us...")
            
            d = defer.Deferred()
            
            def connection_established(peer):
                if peer == expect_preconnect:
                    d.callback(None)
            rpcsystem.connection_established = connection_established
            yield twistit.timeout_deferred(d, comm_setup_timeout,  "Timeout while waiting for parent to open connection.")
            
            logger.info("Parent connected to us.")
                
        logger.info("Calling `init` of parent...")
        d = init(get_function_url_url, reset_url, stop_url)
        yield twistit.timeout_deferred(d, comm_setup_timeout, 
                                       "Timeout during first attempt to communicate with parent.")
        logger.info("Process has started.")
        
        yield watchdog.start()
        
        logger.info("Shutting down...")
        
        logger.debug("Closing RPC system...")
        yield twistit.timeout_deferred(rpcsystem.close(), comm_setup_timeout, 
                                       "Timeout while closing RPC System")
        
        logger.info("Telling the reactor to stop...")
        reactor.stop()  # @UndefinedVariable
        
    except:
        logger.exception("Error while initializing process.")
        
    finally:
        logger.debug("Calling kill function...")
        kill_function()
        logger.info("Process will now exit")
    

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
import time
if hasattr(signal, "SIGHUP"):
    signal.signal(signal.SIGHUP, lambda *args:os._exit(1))
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
codepath = os.path.abspath("code.zip")
sys.path.insert(0, codepath)
time.sleep(0.1)
import {kill_module}
try:
    import {this_module}
    import {factory_module}
    from twisted.internet import reactor
    {this_module}._boot(
        {factory_module}.{factory_name}, 
        {factory_kwargs},
        {init_url}, 
        {kill_module}.{kill_name},
        {comm_setup_timeout},
        {watchdog_timeout},
        {expect_preconnect})
    reactor.run()
except:
    traceback.print_exc()
    {kill_module}.{kill_name}()
    
"""
