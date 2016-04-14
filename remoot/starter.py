# Copyright (C) 2014 Stefan C. Mueller


"""
Starts processes on this or another machine.

The starter returns a `Process` with the following API:
    
.. py:class:: Process

    Objects representing processes have the following attributes and methods:

    .. py:method:: send_stdin(data)
   
        Sends data to the standard input of this process.
   
        :param str data: Data to send.
      
    .. py:method:: kill()
    
        Attempts to forcefully end the process if it hasn't ended already.
        In any case, it cleans up all resources locally bound to it.
        The deferred calls back once finished.
        
    .. py:attribute:: stdout
    
        :class:`~remoot.deferutils.Event` fired when the process produced
        standard output data.
        
    .. py:attribute:: stderr
    
        :class:`~remoot.deferutils.Event` fired when the process produced
        standard error data.
        
    .. py:attribute:: exited
    
        :class:`~remoot.deferutils.Event` fired when the process has ended
          with a :class:`Failure` indiciating the reason, or `None` if
          the process has exited normally.
    
    .. py:attribute:: hostname
    
        IP or host name on which the process has been started.
    
"""

import re
import time
import os.path
import tempfile
import pkgutil
import atexit
import logging

from libcloud.compute.types import Provider, NodeState
from libcloud.compute.providers import get_driver
from libcloud.compute.base import NodeImage, NodeSize, NodeAuthSSHKey

from twisted.internet import defer, reactor, threads, task, error, protocol

from remoot import ssh
from remoot import deferutils
from twisted.python import failure

logger = logging.getLogger(__name__)

class Starter(object):
    
    def start(self, command, fileset={}):
        """
        Starts one or more processes, potentially on a remote machine.
        
        :param command: Command to execute. The first element has to be the absolute
            path to the executable, followed by zero or more arguments.
        :type command: list of strings
        
        :param dict fileset: Set of files to transfer to the machine on which the
            command is executed. The keys are paths relative to the current working
            directory of the executed command. The values are strings with the file
            content. Not all starters support file transfer.
            
        :returns: The started process.
        :rtype: deferred :class:`remoot.process.Process`.
        """
        raise NotImplementedError("abstract")
        
        
class LocalStarter(Starter):
    """
    Creates a starter that starts processes on the local machine.
    """
    
    def __init__(self, tmp_dir=None):
        self.tmp_dir = tmp_dir

    def start(self, command, fileset={}):
        
        def transfer_files():
            # we use blocking IO to create the files. Twisted's `fdesc` is of no use since
            # `O_NONBLOCK` is ignored on regular files (it is for sockets, really).
            # So we do this in a thread instead.
            for filename, content in fileset.iteritems():
                path = os.path.join(tmp_dir, filename)
                with open(path, 'wb') as f:
                    f.write(content)
                    
                    
        def spawn_process(command, path=None):
            protocol = _LocalProcessProtocol()
            d = defer.maybeDeferred(reactor.spawnProcess, protocol, command[0], command, env=None, path=path)  # @UndefinedVariable
            def started(_):
                return protocol.process_d
            d.addCallback(started)
            return d

                    
        # select directory for fileset. Don't do this in
        # in __init__ as the starter might be sent to a different
        # machine.
        if self.tmp_dir:
            tmp_dir = self.tmp_dir
        else:
            tmp_dir = tempfile.gettempdir()
        
        if fileset:
            d = threads.deferToThread(transfer_files)
        else:
            d = defer.succeed(None)
        
            
        d.addCallback(lambda _:spawn_process(command, path=tmp_dir))
        return d
        
        
        
class _LocalProcess(object):
    def __init__(self, transport):
        self._transport = transport
        self.stdout = deferutils.Event()
        self.stderr = deferutils.Event()
        self.exited = deferutils.Event()
        self.hostname = "localhost"

    def send_stdin(self, data):
        self._transport.write(data)
        
    def kill(self):
        try:
            self._transport.signalProcess('KILL')
        except error.ProcessExitedAlready:
            return defer.succeed(None)
        d = self.exited.next_event()
        def onerror(reason):
            # It's hardly a surprise that the process exits with a 
            # non-zero exit code at this point.
            reason.trap(error.ProcessTerminated)
        d.addErrback(onerror)
        return d

class _LocalProcessProtocol(protocol.ProcessProtocol):
    def __init__(self):
        self.process = None
        self.process_d = defer.Deferred()
        
    def connectionMade(self):
        self.process = _LocalProcess(self.transport)
        self.process_d.callback(self.process)
        
    def errReceived(self, data):
        self.process.stderr.fire(data)
    
    def outReceived(self, data):
        self.process.stdout.fire(data)
        
    def processEnded(self, status):
        if status and status.check(error.ProcessDone):
            status = None
        self.process.exited.fire(status)
        
class SSHStarter(Starter):
    """
    Creates a starter that starts processes on remote machines.
    """

    def __init__(self, hostname, username, password=None, private_key_files=[], private_keys=[], tmp_dir="/tmp"):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.private_keys = map(_read_file, private_key_files) + list(private_keys)
        self.tmp_dir = tmp_dir


    def start(self, command_args, fileset={}):

        basedir = self.tmp_dir
        
        command = " ".join(_bash_escaping(arg) for arg in command_args)
        if fileset:
            command = "cd {basedir} ; exec {command}".format(basedir=_bash_escaping(basedir),
                                                                             command=command)

        def on_connected(connection):
                       
            def transmit_file(sftp, path, content):
                abspath = basedir + "/" + path
                logger.debug("Transferring %s..." % abspath)
                d = sftp.write_file(abspath, content)
                d.addCallback(lambda _:sftp)
                return d
            
            def start_command(ignore):
                logger.debug("Executing command on %s: %s" % (self.hostname, command))
                return connection.execute(command)
            
            def command_started(proc):
                logger.debug("Command on %s is running" % self.hostname)
                return _SSHProcess(proc, connection, self.hostname)
                       
            def error_while_connection_open(failure):
                logger.debug("Closing connection to %s due to error." % self.hostname)
                d = connection.close()
                d.addBoth(lambda _:failure)
                return d
 
            if fileset:
                logger.debug("Opening SFTP session to %s" % self.hostname)
                d = connection.open_sftp()
                for path, content in fileset.iteritems():
                    d.addCallback(lambda sftp:transmit_file(sftp, path, content))
                d.addCallback(lambda sftp:sftp.close())
            else:
                d = defer.succeed(None)
            d.addCallback(start_command)
            d.addCallback(command_started)
            d.addErrback(error_while_connection_open)
            return d
            
        logger.debug("Opening SSH connection %s @ %s" % (self.username, self.hostname))
            
        d = ssh.connect(self.hostname, self.username, self.password, private_keys=self.private_keys)
        d.addCallback(on_connected)
        
        return d
        
class _SSHProcess(object):
    """
    Wrapper around `ssh._SSHProcess`. The process from the `ssh` module
    correctly assumes that other processes and file transfers might be
    running over the same underlying connection, thous does not close
    the connection on exit. This process does exactly that, assuming
    that the connection only exists for the purpose of this one process.
    We also support `kill` after a fashion: We close the connection,
    the actual process will receive SIGHUB, and we can free all local
    resources.
    """
    def __init__(self, sshprocess, sshconnection, hostname):
        self._sshprocess = sshprocess
        self._sshconnection = sshconnection
        self.hostname = hostname
        
        self.stdout = self._sshprocess.stdout
        self.stderr = self._sshprocess.stderr
        
        self.exited = deferutils.Event()
        self._sshprocess.exited.add_callback(self._on_exit)
        
    def kill(self):
        """
        Closes the SSH connection. The process will only receive SIGHUB,
        hopefully this is enough to make it exit.
        """
        logging.warn("Killing ssh process")
        self._sshconnection.close()
        d = self.exited.next_event()
        def onerror(reason):
            # It's hardly a surprise that the process exits with a 
            # non-zero exit code or a lost connection.
            reason.trap(error.ProcessTerminated, error.ConnectionLost)
        d.addErrback(onerror)
        return d
        
    def _on_exit(self, reason):
        d = self._sshconnection.close()
        def on_closed(_):
            self.exited.fire(reason)
        d.addCallback(on_closed)


def _bash_escaping(string):
    """
    Escapes and puts quotes around an arbitrary byte sequence so that the given
    string will be interpreted as a single string in a bash command line.
    
    Escaped strings can be concatenated (separted by spaces) to form
    a bash command. For example to execute an arbitrary python script one could
    execute the following bash command:
    
       "python -c %s" % bash_escaping(my_script)
       
    This will work even if `string` contains new-lines, quotes or other
    special characters.
    """

    def octal(ascii_code):
        """
        Returns the octal string of the given ascii code.
        Leading zeros are added to pad to three characters.
        """
        if ascii_code < 0 or ascii_code > 255:
            raise ValueError("Not an ASCII code")
        least_sig = ascii_code % 8
        mid_sig = (ascii_code >> 3) % 8
        most_sig = (ascii_code >> 6) % 8
        return "%s%s%s" % (most_sig, mid_sig, least_sig)

    from cStringIO import StringIO
    strfile = StringIO()
    strfile.write("$'")
    for char in string:
        if char >= 'a' and char <= 'z':
            strfile.write(char)
        elif char >= 'A' and char <= 'Z':
            strfile.write(char)
        elif char >= '0' and char <= '9':
            strfile.write(char)
        elif char == "." or char == " ":
            strfile.write(char)
        else:
            strfile.write('\\')
            strfile.write(octal(ord(char)))
    strfile.write("'")
    return strfile.getvalue()

def _read_file(path):
    with open(path) as f:
        return f.read()


class EC2Starter(Starter):
    
    _ip_regex = re.compile("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+")
    _ip_attempts = 1200
    _ip_wait = 0.5
    _login_attempts = 20
    _login_wait = 0.5
    
    def __init__(self, username, provider, provider_keyid, provider_key, image_id, size_id, public_key_file, private_key_file, tmp_dir, **provider_kwargs):
        self.username = username
        self.provider = provider
        self.provider_keyid = provider_keyid
        self.provider_key = provider_key
        self.image_id = image_id
        self.size_id = size_id
        self.public_key = _read_file(public_key_file)
        self.private_key = _read_file(private_key_file)
        self.tmp_dir = tmp_dir
        self.provider_kwargs = provider_kwargs
        if self.provider == None:
            self.provider = Provider.DUMMY

    
    def start(self, command, fileset={}):
        
        self._add_certificates()
        
        def public_ips_received(ips):
            
            def login_retry(reason):
                return task.deferLater(reactor, self._login_wait, sshstarter.start, command, fileset)
            
            ip = ips[0]
            sshstarter = SSHStarter(ip, username=self.username, private_keys=[self.private_key], tmp_dir=self.tmp_dir)
            d = sshstarter.start(command, fileset)
            for _ in range(self._login_attempts):
                d.addErrback(login_retry)
            return d
        
        def node_started(value):
            node, driver = value
            
            def process_started(process):
                return _EC2Process(process, driver, node, True)
            
            d = public_ips_received(node.public_ips)
            d.addCallback(process_started)
            return d
        
        d = threads.deferToThread(self._start_node)
        d.addCallback(node_started)
        return d
    
    
    def _start_node(self):
        """
        Starts a node. This method is blocking.
        """

        def _wait_for_public_ips(node):
            """
            Waits until the public ips of the node are available. This method is blocking.
            """
            for _ in range(self._ip_attempts):
                
                # replace node with the updated version of it.
                candidates = [n for n in driver.list_nodes() if n.uuid == node.uuid]
                if candidates:
                    node = candidates[0]
                    if node.state == NodeState.RUNNING and node.public_ips and all(map(self._is_valid_ip, node.public_ips)):
                        break
                time.sleep(self._ip_wait)
            else:
                raise ValueError("No public IP after %s seconds" % (self._ip_attempts * self._ip_wait))
            return node, driver

        driver_class = get_driver(self.provider)
        driver = driver_class(self.provider_keyid, self.provider_key, **self.provider_kwargs)
        
        image = NodeImage(id=self.image_id, name=None, driver=driver_class)
        size = NodeSize(id=self.size_id, 
                        name=None, 
                        ram=None, 
                        disk=None, 
                        bandwidth=None, 
                        price=None, 
                        driver=driver_class)

        auth = NodeAuthSSHKey(self.public_key)
        
        node = driver.create_node(name='test-node', image=image, size=size, auth=auth)
        
        node = _wait_for_public_ips(node)
        
        return node

    def _is_valid_ip(self, ip):
        """
        Checks if the given string looks a bit like an IPv4 addresss.
        """
        return bool(self._ip_regex.match(ip))
    
    
    _certiciates_injected = False
    
    @classmethod
    def _add_certificates(cls):
        if cls._certiciates_injected:
            return
        cls._certiciates_injected = True
        
        import libcloud.security
        
        ca_bundle = pkgutil.get_data("remoot", "ca_bundle.crt")
    
        tmpdir = tempfile.mkdtemp()
        cert_file = os.path.join(tmpdir, "ca-bundle.crt")
        
        with os.fdopen(os.open(cert_file, os.O_WRONLY | os.O_CREAT, 0600), 'w') as f:
            f.write(ca_bundle)
            
        def cleanup():
            os.unlink(cert_file)
            os.rmdir(tmpdir)
        
        atexit.register(cleanup)
    
        libcloud.security.CA_CERTS_PATH.append(cert_file)
    

class _EC2Process(object):
    """
    Wrapper around the SSH process. Takes care of terminating the node.
    """
    
    def __init__(self, proc, driver, node, terminate_on_exit):
        """
        :param proc: Process running on node.
        :param driver: Driver that started the node.
        :param node: Node the given process is running on.
        :param terminate_on_exit: If true, the node will be
           terminated once the process exits, if not the
           node will only terminate if the process is killed
           with :meth:`kill`.
        """
        
        self.exited = deferutils.Event()
        self.stdout = proc.stdout
        self.stderr = proc.stderr
        
        self._process = proc
        self._driver = driver
        self._node = node
        self._has_exited = False
        self._terminate_on_exit = terminate_on_exit

        if terminate_on_exit:
            self._process.exited.add_callback(self._terminate)
        else:
            def on_exit(reason):
                self._has_exited = True
                self.exited.fire(reason)
            self._process.exited.add_callback(on_exit)

    @property
    def hostname(self):
        return self._process.hostname

    def kill(self):
        if self._terminate_on_exit:
            d = self._process.kill()
            def process_dead(_):
                return self._terminate(error.ProcessTerminated(None, 9, None))
            d.addBoth(process_dead)
            return d
        else:
            return self._process.kill()
    
    def send_stdin(self, data):
        return self._process.send_stdin(data)
    
    def _terminate(self, terminate_reason):
        
        def destroy_node():
            self._driver.destroy_node(self._node)
            
        def destroyed_success(_):
            if not self._has_exited:
                self._has_exited = True
                self.exited.fire(terminate_reason)
                
        def destroyed_error(destroy_reason):
            if not self._has_exited:
                self._has_exited = True
                self.exited.fire(destroy_reason)
        
        d = threads.deferToThread(destroy_node)
        d.addCallbacks(destroyed_success, destroyed_error)
        return d

