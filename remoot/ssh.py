# Copyright (C) 2014 Stefan C. Mueller

import struct
import StringIO as stringio
import logging

from twisted.conch.ssh import transport
from twisted.conch.ssh import keys, userauth
from twisted.conch.ssh import connection, channel, common
from twisted.conch.endpoints import AuthenticationFailed
from twisted.internet import defer
from twisted.internet import protocol, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet import error
from twisted.python.failure import Failure
from twisted.conch.ssh import filetransfer

import deferutils
from twisted.python import failure

logger = logging.getLogger(__name__)

"""
Simple API for SSH and SFTP using `twisted.conch`. Conch is a rather low-level
API, this one has less features but is easier to use.

.. py:class:: SSHConnection

    Objects representing a connection to an SSH server.

    .. py:method:: execute(data)
   
        Executes a command on the server.
        
        This connection stays open after the commmand has
        been executed. If this connection is closed,
        contact to any still running processes is lost.
        The processes on the remote machine will typically
        receive a `SIGHUB` signal. 
        The :class:`process.Process` instance will report `exited` to
        its listeners with :class:`twisted.internet.error.ConnectionLost` 
        as reason.
        
        :param str command: Command to execute.
        
        :returns: Deferred :class:`process.Process` instance.
           A listener must be added without delay or events
           may not be observed.
      
    .. py:method:: open_sftp()
    
        Opens an SFTP connection to access the server's file
        system.
        
        This connection stays open after the sftp connection
        has been closed. If this connection is closed any
        still open sftp connection is closed as well.
                
        :returns: Deferred :class:`SFTPConnection`.
           A listener must be added without delay or events
           may not be observed.
           
    .. py:method:: close()
    
        Closes the connection.
        
        :returns: Deferred reporting when closed.
        
    .. py:attribute:: closed
    
        :class:`~remoot.deferutils.Event` fired when the connection has closed
          with a :class:`Failure` indiciating the reason, or `None` 
          if the connection was closed normally.
          
          
          
          
.. py:class:: SFTPConnection

    Objects representing a connection to an SSH server.

    .. py:method:: read_file(path)
 
        Reads the content of a file.
        
        :param str path: Absolute path to the file.
        
        :returns: Deferred content of the file as a string.

    
    .. py:method:: write_file(path, content)
    
        Creates a file.
        
        :param str path: Absolute path to the file.
        
        :param str content: Content to be written into the file.

    .. py:method:: delete_file(path)

        Deletes a file.
        
        :param str path: Absolute path to the file.
        
        :returns: Deferred reporting when completed.

    .. py:method:: create_directory(path)

        Creates a directory.
        
        :param str path: Absolute path to the directory.
        
        :returns: Deferred reporting when completed.

    .. py:method:: delete_directory(path)

        Deletes an empty directory.
        
        :param str path: Absolute path to the directory.
        
        :returns: Deferred reporting when completed.

    .. py:method:: list_directory(path)

        Lists the files and directories within a directory.
        
        :param str path: Absolute path to the directory.
        
        :returns: Deferred list of `(name, isdir)` tuples. `name` is
          relative to the directory (only the name, no path) and 
          `isdir` is `True` for directories and `False` for files.
          
    .. py:method:: close()

        Closes this connection.
        
        :returns: Deferred reporting when closed.

        
    .. py:attribute:: closed
    
        :class:`~remoot.deferutils.Event` fired when the connection has closed
          with a :class:`Failure` indiciating the reason, or `None` 
          if the connection was closed normally.
          
"""



def connect(hostname, username, password=None, private_key_files=[], private_keys=[], connection_timeout=30):
    """
    Opens a SSH connection to a remote server.
    
    :param str hostname: Hostname or IP of the server.
    
    :param str username: Username to authenticate with.
    
    :param str password: Password to autenticate with or `None` (default) 
      for public/private-key authentification only.
      
    :param private_key_files: List of paths to files containing private keys to attempt.
    
    :param private_keys: List of strings containing the private keys to attempt.
    
    :returns: Deferred :class:`SSHConnection`.
    """
    private_keys = [keys.Key.fromFile(f) for f in private_key_files] + [keys.Key.fromString(s) for s in private_keys]
    factory = _ClientTransportFactory(username, password, private_keys)
    
    endpoint = TCP4ClientEndpoint(reactor, hostname, 22)
    
    logger.debug("Opening TCP connection to %s" % hostname)
    d = endpoint.connect(factory)
    
    client_transport = [None]
    
    def got_transport(ct):
        logger.debug("TCP connection to %s is opened." % hostname)
        client_transport[0] = ct
        return ct.get_client_connection()
    
    def got_connection(client_connection):
        logger.debug("SSH client connection to %s is opened." % hostname)
        ct = client_transport[0]
        conn = _SSHConnectionImpl(ct, client_connection)
        client_connection.connection = conn
        return conn
        
    d.addCallback(got_transport)
    d.addCallback(got_connection)
    return d


def execute(command, hostname, username, password=None, private_key_files=[], private_keys=[]):
    """
    Execute a single command on the remote machine. 
    
    This opens an SSH connection
    for the execution and closes it automatically afterwards. 
    Consider using :func:`connect` if several commands have to be executed
    or file operations are required as well.
    """
    
    def connected(conn):
        
        def started(proc):
            proc.kill = conn.close
            proc.exited.add_callback(lambda _:conn.close())
            return proc
        
        d = conn.execute(command)
        d.addCallback(started)
        return d

        
    conn_defer = connect(hostname, username, password, private_key_files, private_keys)
    conn_defer.addCallback(connected)
    return conn_defer


def sftp(hostname, username, password=None, private_key_files=[], private_keys=[]):
    """
    Opens an SFTP connection to a remote machine.
    
    This opens an SSH connection
    for the SFTP session and closes it automatically afterwards. 
    Consider using :func:`connect` if commands have to be executed as well.
    """
    
    def connected(conn):
        
        def opened(sftp):
            sftp.closed.add_callback(lambda _:conn.close())
            return sftp
        
        d = conn.open_sftp()
        d.addCallback(opened)
        return d
        
    conn_defer = connect(hostname, username, password, private_key_files, private_keys)
    conn_defer.addCallback(connected)
    return conn_defer


class _SSHConnectionImpl(object):
    
    def __init__(self, transport, connection):
        self._transport = transport
        self._connection = connection
        self.closed = deferutils.Event()
    
    def execute(self, command):
        def on_executed(ignored):
            logger.debug("Channel for 'execute' is open.")
            process = _SSHProcess(channel)
            channel.process = process
            return process
        
        logger.debug("Opening channel for 'execute'.")
        channel = _ExecutionChannel(command=command, conn=self._connection)
        self._connection.openChannel(channel)
        d = channel.executed
        d.addCallback(on_executed)
        return d
    
    def open_sftp(self):
        logger.debug("Opening channel for 'sftp'.")
        channel = _SFTPChannel(conn=self._connection)
        self._connection.openChannel(channel)
        
        def success(result):
            logger.debug("Channel for 'sftp' is open.")
            return result
        
        d = channel.sftp_connection_d
        d.addCallback(success)
        return d
    
    def close(self):
        logger.debug("Closing connection")
        self._transport.loseConnection()
        return self._transport.get_client_closed()

    def _notify_closed(self, reason):
        self.closed.fire(reason)
        
            
class _SFTPConnectionImpl(object):
    
    JUNK_SIZE = 64*1024
     
    def __init__(self, client, channel):
        self._client = client
        self._channel = channel
        self.closed = deferutils.Event()
 
    def read_file(self, path):
        bfr = stringio.StringIO()
        
        def eof_handling(error):
            if error.type == EOFError:
                return ""
            else:
                return error
        
        def file_opened(file_interface):
            def injest(data):
                if data:
                    bfr.write(data)
                    d = defer.maybeDeferred(file_interface.readChunk, bfr.len, self.JUNK_SIZE)
                    d.addCallback(injest)
                    d.addErrback(eof_handling)
                    return d
                else:
                    return
                
            def read_completed(_):
                return file_interface.close()
                
            def closed(_):
                return bfr.getvalue()
                
            d = defer.maybeDeferred(file_interface.readChunk, bfr.len, self.JUNK_SIZE)
            d.addCallback(injest)
            d.addCallback(read_completed)
            d.addCallback(closed)
            return d
        
        d = self._client.openFile(path, filetransfer.FXF_READ, {})
        d.addCallback(file_opened)
        return d
    
    def write_file(self, path, content):
        def file_opened(file_interface):
            
            def write_junk(index):
                start_pos = index * self.JUNK_SIZE
                end_pos = (index + 1) * self.JUNK_SIZE
                junk = content[start_pos:end_pos]
                d = defer.maybeDeferred(file_interface.writeChunk, start_pos, junk)
                if end_pos < len(content):
                    d.addCallback(lambda _: write_junk(index + 1))
                return d
            
            def data_written(_):
                return file_interface.close()

            d = write_junk(0)
            d.addCallback(data_written)
            return d

        
        d = self._client.openFile(path, filetransfer.FXF_CREAT | filetransfer.FXF_WRITE, {})
        d.addCallback(file_opened)
        return d
 
    def delete_file(self, path):
        return self._client.removeFile(path)
 
    def create_directory(self, path):
        return self._client.makeDirectory(path, {})
     
    def delete_directory(self, path):
        return self._client.removeDirectory(path)
     
    def list_directory(self, path):
        def opened(iterable):
            
            def ignore_stop(error):
                # Should be StopIteration, but due to
                # https://twistedmatrix.com/trac/ticket/7322
                # an EOFError is produced. We accept both, so that
                # we are ready when the bug is fixed.
                if error.type == StopIteration or error.type == EOFError:
                    return None
                return error
            
            def injest_item(item):
                if item:
                    items.append(item)
                d = defer.maybeDeferred(iterator.next)
                d.addCallback(injest_item)
                d.addErrback(ignore_stop)
                return d
            
            def read_all(_):
                return defer.maybeDeferred(iterable.close)

            def iterator_closed(_):
                return [(item[0], item[1][0] == 'd' if item[1] else False) for item in items if item[0] != "." and item[0] != ".."]
            
            iterator = iter(iterable)
            items = []
            d = injest_item(None)
            d.addCallback(read_all)
            d.addCallback(iterator_closed)
            return d
            
        d = self._client.openDirectory(path)
        d.addCallback(opened)
        return d
     
    def close(self):
        return self._channel.close()

    def _notify_closed(self, reason):
        self.closed.fire(reason)
            
            

class _SSHProcess(object):
    
    def __init__(self, channel):
        self._channel = channel
        self.stdout = deferutils.Event()
        self.stderr = deferutils.Event()
        self.exited = deferutils.Event()
    
    def send_stdin(self, data):
        self._channel.write(data)
    
    def kill(self):
        # Sending signals is not supported by the OpenSSH server:
        # https://bugzilla.mindrot.org/show_bug.cgi?id=1424
        # self._channel.conn.sendRequest(self._channel, 'signal', common.NS('KILL'))
        raise NotImplementedError("Cannot kill a process through SSH. Simply closing the connection may help, though")

    def _notify_stdout_received(self, data):
        self.stdout.fire(data)
            
    def _notify_stderr_received(self, data):
        self.stderr.fire(data)

    def _notify_exited(self, exit_code):
        self.exited.fire(exit_code)


class _ClientTransportFactory(protocol.ClientFactory):
    
    def __init__(self, username, password, private_keys):
        self.username = username
        self.password = password
        self.private_keys = private_keys
    
    def buildProtocol(self, addr):
        return _ClientTransport(self.username, self.password, self.private_keys)


class _ClientTransport(transport.SSHClientTransport):
    
    def __init__(self, username, password, private_keys):
        self.username = username
        self.password = password
        self.private_keys = private_keys
        
        self._state = "new"
        self._client_connection = None
        self._client_connection_deferreds = []
        self._connection_lost_deferreds = []
        
    def get_client_connection(self):
        if self._client_connection is None:
            d = defer.Deferred()
            self._client_connection_deferreds.append(d)
            return d
        else:
            return defer.succeed(self._client_connection)
        
    def get_client_closed(self):
        if self._state == "closed":
            return defer.succeed(None)
        else:
            d = defer.Deferred()
            self._connection_lost_deferreds.append(d)
            return d

    def verifyHostKey(self, pubKey, fingerprint):
        logger.debug("Received peers key.")
        return defer.succeed(1)

    def connectionSecure(self):
        self._state = "auth"
        logger.debug("Connection is secure. Authentificating...")

        cc = _ClientConnection()
        d = cc.service_started_deferred
        d.addCallback(self._client_connection_started)
        self._client_auth = _ClientUserAuth(self.username, cc, self.password, self.private_keys)
        self.requestService(self._client_auth)
        
    def connectionMade(self):
        logger.debug("Connection made.")
        transport.SSHClientTransport.connectionMade(self)
        logger.debug("Connection made2.")
        
    def connectionLost(self, reason):
        logger.debug("Connection lost: %s" % reason.getTraceback())
        transport.SSHClientTransport.connectionLost(self, reason)
        
        if reason.check(error.ConnectionDone):
            if self._state == "auth":
                if self._client_auth.auth_failed_message:
                    reason = Failure(AuthenticationFailed(self._client_auth.auth_failed_message))
                else:
                    reason = Failure(AuthenticationFailed("Connection lost while authenticating"))
                
        self._state = "closed"
                
        ds = self._client_connection_deferreds
        self._client_connection_deferreds = []
        for d in ds:
            d.errback(reason)
            
        if self._client_connection:
            self._client_connection.connectionLost(reason)
            
        ds = self._connection_lost_deferreds
        self._connection_lost_deferreds = []
        for d in ds:
            d.callback(None)
        
    def _client_connection_started(self, cc):
        self._state = "running"
        self._client_connection = cc
        
        
        ds = self._client_connection_deferreds
        self._client_connection_deferreds = []
        for d in ds:
            d.callback(cc)


class _ClientUserAuth(userauth.SSHUserAuthClient):
    
    def __init__(self, user, instance, password, private_keys):
        userauth.SSHUserAuthClient.__init__(self, user, instance)
        self.password = password
        self.private_keys = list(private_keys)
        self._password_sent = False
        self.auth_failed_message = None
        
    def serviceStarted(self):
        
        # monkey-patch self.transport so that we get informed when we fail
        # to authenticate.
        def disconnect_wrapper(typeid, message):
            if typeid == transport.DISCONNECT_NO_MORE_AUTH_METHODS_AVAILABLE:
                self.auth_failed_message = message
            return original_sendDisconnect(typeid, message)
        original_sendDisconnect = self.transport.sendDisconnect
        self.transport.sendDisconnect = disconnect_wrapper
        
        
        userauth.SSHUserAuthClient.serviceStarted(self)

    def getPassword(self, prompt = None):
        if self.password and not self._password_sent:
            self._password_sent = True
            return defer.succeed(self.password)
        else:
            return None

    def getPublicKey(self):
        if self.private_keys:
            self.key = self.private_keys.pop(0)
            return defer.succeed(self.key.public())
        else:
            return None

    def getPrivateKey(self):
        return defer.succeed(self.key)
        
        

class _ClientConnection(connection.SSHConnection):
        
    def __init__(self):
        connection.SSHConnection.__init__(self)
        self.service_started_deferred = defer.Deferred()
        self.connection = None

    def serviceStarted(self):
        self.service_started_deferred.callback(self)

    def connectionLost(self, reason):
        if reason.check(error.ConnectionDone):
            self.connection._notify_closed(None)
        else:
            self.connection._notify_closed(reason)

class _ExecutionChannel(channel.SSHChannel):
    
    name = 'session'
    
    def __init__(self, command, localWindow=0, localMaxPacket=0, 
        remoteWindow=0, remoteMaxPacket=0, 
        conn=None, data=None, avatar=None):
        channel.SSHChannel.__init__(self, localWindow=localWindow, 
                                    localMaxPacket=localMaxPacket, 
                                    remoteWindow=remoteWindow, 
                                    remoteMaxPacket=remoteMaxPacket, 
                                    conn=conn, data=data, avatar=avatar)
     
        self._command = command
        
        self.process = None
        
        #: Status passes through:
        #: "new": instance created, but channel not yet opened.
        #: "starting": channel open, 'exec' request sent.
        #: "running": command is executing
        #: "stopped": command has stopped, or an error occured.
        self.status = "new"
        
        #: callsback as soon as we reach "running".
        self.executed = defer.Deferred()
     
    def channelOpen(self, specificData):
        
        def success(result):
            self.status = "running"
            self.executed.callback(result)
        def failed(reason):
            self.status = "stopped"
            self.executed.errback(reason)
        
        d = self.conn.sendRequest(self, 'exec', common.NS(self._command), wantReply=True)
        self.status = "starting"
        d.addCallbacks(success, failed)

    def openFailed(self, reason):
        channel.SSHChannel.openFailed(self, reason)
        self.status = "stopped"
        self.executed.errback(reason)
        
    def dataReceived(self, data):
        self.process._notify_stdout_received(data)
    
    def extReceived(self, dataType, data):
        if dataType==connection.EXTENDED_DATA_STDERR:
            self.process._notify_stderr_received(data)

    def closed(self):
        channel.SSHChannel.closed(self)
        # Delay so that we don't accidentally overtake the delayed calls in
        # `request_exit_status()` and `request_exit_signal()`
        try:
            raise error.ConnectionLost()
        except:
            reactor.callLater(0, self._exited, failure.Failure())  # @UndefinedVariable

    def request_exit_status(self, data):
        (status,) = struct.unpack('>L', data)

        # Delay informing the listener. Sometimes `dataReceived` is invoked
        # after `request_exit_status` which is really strange.
        # A `callLater` seems to help delay things so that twisted can forward buffered data.
        # Not sure if this is sufficient in all cases.
        if status == 0:
            result = None
        else:
            try:
                raise error.ProcessTerminated(status, None, None)
            except:
                result = failure.Failure()
        reactor.callLater(0, self._exited, result)  # @UndefinedVariable

    def request_exit_signal(self, data):
        (signal,) = struct.unpack('>L', data)

        # Delay informing the listener. Sometimes `dataReceived` is invoked
        # after `request_exit_status` which is really strange.
        # A `callLater` seems to help delay things so that twisted can forward buffered data.
        # Not sure if this is sufficient in all cases.
        try:
            raise error.ProcessTerminated(None, signal, None)
        except:
            reactor.callLater(0, self._exited, failure.Failure())  # @UndefinedVariable
        
    def _exited(self, reason):
        if self.status == "running":
            self.status = "stopped"
            self.process._notify_exited(reason)
            self.process = None
        elif self.status == "starting" or self.status == "new":
            self.status = "stopped"
            self.executed.errback(reason)
            self.process = None

class _SFTPChannel(channel.SSHChannel):
     
    name = 'session'
     
    def __init__(self, *args, **kwargs):
        channel.SSHChannel.__init__(self, *args, **kwargs)
        self.is_closed_d = defer.Deferred()
        self.sftp_connection = None
        self.sftp_connection_d = defer.Deferred()
      
    def channelOpen(self, specificData):
        def sftp_open(result):
            client = filetransfer.FileTransferClient()
            client.makeConnection(self)
            self.dataReceived = client.dataReceived
            self.sftp_connection = _SFTPConnectionImpl(client, self)
            return self.sftp_connection
        d = self.conn.sendRequest(self, 'subsystem', common.NS('sftp'), wantReply=True)
        d.addCallback(sftp_open)
        d.chainDeferred(self.sftp_connection_d)

    def close(self):
        self.conn.sendClose(self)
        return self.is_closed_d

    def closed(self):
        channel.SSHChannel.closed(self)
        if self.sftp_connection is not None:
            self.sftp_connection._notify_closed(error.ConnectionLost())
            self.sftp_connection = None
        self.is_closed_d.callback(True)
