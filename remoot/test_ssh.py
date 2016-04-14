# Copyright (C) 2014 Stefan C. Mueller

import unittest
import StringIO as stringio

from twisted.internet import defer

from utwist import with_reactor

import ssh
import uuid
import test_credentials
from twisted.internet.error import ConnectionLost
from twisted.conch.endpoints import AuthenticationFailed



class TestSSH(unittest.TestCase):
    """
    Unit-tests for the SSH client
    """
    
    @classmethod
    def setUpClass(cls):
        test_credentials.load_credentials()
    
    @with_reactor
    def test_connect_passwd(self):
        
        def on_connected(conn):
            d = conn.closed.next_event()
            conn.close()
            return d

        conn = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           password = test_credentials.ssh_password)

        conn.addCallback(on_connected)
        return conn
    
    @with_reactor
    def test_connect_wrong_passwd(self):

        def on_connected(conn):
            return conn.close()
        
        def on_error(reason):
            if not reason.check(AuthenticationFailed):
                self.fail("Expected AuthenticationFailed")
        
        conn = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           password = "invalid password")

        conn.addCallbacks(on_connected, on_error)
        
        return conn
        
    @with_reactor
    def test_connect_key(self):
        

        def on_connected(conn):
            d = conn.closed.next_event()
            conn.close()
            return d
        
        conn = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        conn.addCallback(on_connected)
        return conn
    
    @with_reactor
    def test_connect_key_string(self):
        

        def on_connected(conn):
            d = conn.closed.next_event()
            conn.close()
            return d
        
        conn = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_keys=[file(test_credentials.ssh_privatekey).read()])
        
        conn.addCallback(on_connected)
        return conn
    
    @with_reactor
    def test_conn_execute(self):
        
                
        def on_connected(conn):
            conns.append(conn)
            return conn.execute("echo 'hello'")
            
        def on_executed(process):
            process.stdout.add_callback(data.append)
            return process.exited.next_event()
            
        def on_exited(_):
            self.assertTrue("hello" in "".join(data), msg="Expected to find 'hello' in output. Got: %s" % repr(data))
            return conns[0].close()
        
        data = []
        conns =[]
        conn_defer = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        
        conn_defer.addCallback(on_connected)
        conn_defer.addCallback(on_executed)
        conn_defer.addCallback(on_exited)
        return conn_defer


    @with_reactor
    def test_conn_execute_disconnect(self):
        
        def on_connected(conn):
            conns.append(conn)
            return conn.execute("sleep 100")
            
        def on_executed(process):
            conns[0].close()
            return process.exited.next_event()
        
        def on_success(_):
            self.fail("Expected failure due to non-zero exit code.")
            
        
        def on_fail(reason):
            self.assertTrue(reason.check(ConnectionLost), "reason is not of type ConnectionLost")

        conns =[]
        conn_defer = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        
        conn_defer.addCallback(on_connected)
        conn_defer.addCallback(on_executed)
        conn_defer.addCallbacks(on_success, on_fail)
        return conn_defer


    @with_reactor
    def test_execute(self):
        
        def on_executed(process):
            process.stdout.add_callback(data.append)
            return process.exited.next_event()
        
        def on_exited(_):
            self.assertTrue("hello" in "".join(data), msg="Expected to find 'hello' in output. Got: %s" % repr(data))
        
        
        data = []
        
        proc_defer = ssh.execute("echo 'hello'", test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        proc_defer.addCallback(on_executed)
        proc_defer.addCallback(on_exited)
        return proc_defer
    

    @with_reactor
    def test_execute_zero_exit(self):
        
        def on_executed(process):
            process.stdout.add_callback(data.append)
            return process.exited.next_event()
        
        def on_exited(reason):
            self.assertIsNone(reason)
        
        data = []
        
        proc_defer = ssh.execute("echo 'hello'", test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        proc_defer.addCallback(on_executed)
        proc_defer.addCallback(on_exited)
        return proc_defer
    
    
    @with_reactor
    def test_execute_stderr(self):


        def on_executed(process):
            process.stderr.add_callback(data.append)
            return process.exited.next_event()
        
        def on_exited(_):
            self.assertTrue("hello" in "".join(data), msg="Expected to find 'hello' in output. Got: %s" % repr(data))
        
        
        data = []
        
        proc_defer = ssh.execute("python -c \"import sys;sys.stderr.write('hello')\"", test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        proc_defer.addCallback(on_executed)
        proc_defer.addCallback(on_exited)
        return proc_defer
    
    @with_reactor
    def test_execute_exitcode_42(self):
        

        def on_executed(process):
            return process.exited.next_event()
            
        def on_success(_):
            self.fail("Expected failure due to non-zero exit code.")
            
        def on_fail(reason):
            self.assertEqual(42, reason.value.exitCode)
        
        proc_defer = ssh.execute("exit 42", test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        proc_defer.addCallback(on_executed)
        proc_defer.addCallbacks(on_success, on_fail)
        return proc_defer
    
    
    @with_reactor
    def test_execute_kill(self):
        
        def on_executed(process):
            d = process.exited.next_event()
            process.kill()
            return d
            
        def on_success(_):
            self.fail("Expected failure due to kill().")
            
        def on_fail(reason):
            self.assertTrue(reason.check(ConnectionLost), "reason is not of type ConnectionLost")
        
        proc_defer = ssh.execute("python -c 'import time;time.sleep(10)'", test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        
        proc_defer.addCallback(on_executed)
        proc_defer.addCallbacks(on_success, on_fail)
        return proc_defer
    
    
    @with_reactor
    def test_conn_sftp(self):
                 
        def on_connected(conn):
            conns.append(conn)
            return conn.open_sftp()
             
        def on_sftp_open(sftp):
            return sftp.close()

        def on_sftp_closed(_):
            return conns[0].close()
         
        conns =[]
        conn_defer = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
         
        conn_defer.addCallback(on_connected)
        conn_defer.addCallback(on_sftp_open)
        conn_defer.addCallback(on_sftp_closed)
        return conn_defer
    
class TestSFTP(unittest.TestCase):
    """
    Unit-tests for the SSH client SFTP protocol
    """
    
    @classmethod
    def setUpClass(cls):
        test_credentials.load_credentials()
    
    def twisted_setup(self):
        
        def on_connected(conn):
            self.connection = conn
            return conn.open_sftp()
             
        def on_sftp_open(sftp):
            self.sftp = sftp

        conn_defer = ssh.connect(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        conn_defer.addCallback(on_connected)
        conn_defer.addCallback(on_sftp_open)
        return conn_defer
    
    def twisted_teardown(self):
        
        def on_sftp_closed(_):
            self.connection.close()
        
        d = self.sftp.close()
        d.addCallback(on_sftp_closed)
        return d
    
    
    
    @with_reactor
    def test_close(self):
        pass
    
    
    @with_reactor
    def test_mkdir(self):
        name = str(uuid.uuid4())
        return self.sftp.create_directory(test_credentials.ssh_tmp_dir + "/" + name)
    
    @with_reactor
    def test_rmdir(self):
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        
        def created(_):
            return self.sftp.delete_directory(path)
        
        d = self.sftp.create_directory(path)
        d.addCallback(created)
        return d
    
    @with_reactor
    def test_write_file(self):
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        return self.sftp.write_file(path, "Hello World!")


    @with_reactor
    def test_write_read_file(self):
        
        def file_created(_):
            return self.sftp.read_file(path)
        
        def read(data):
            self.assertEqual("Hello World!", data)
        
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        d = self.sftp.write_file(path, "Hello World!")
        d.addCallback(file_created)
        d.addCallback(read)
        return d


    @with_reactor
    def test_write_read_exactly_junksize(self):
        
        content = "x" * ssh._SFTPConnectionImpl.JUNK_SIZE
        
        def file_created(_):
            return self.sftp.read_file(path)
        
        def read(data):
            self.assertEqual(content, data)
        
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        d = self.sftp.write_file(path, content)
        d.addCallback(file_created)
        d.addCallback(read)
        return d


    @with_reactor
    def test_write_read_larger_junksize(self):
        
        content = "x" * (ssh._SFTPConnectionImpl.JUNK_SIZE + 10)
        
        def file_created(_):
            return self.sftp.read_file(path)
        
        def read(data):
            self.assertEqual(content, data)
        
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        d = self.sftp.write_file(path, content)
        d.addCallback(file_created)
        d.addCallback(read)
        return d


    @with_reactor
    def test_write_read_large_file(self):
        content = "x" * (1024 * 1000)
        
        def file_created(_):
            return self.sftp.read_file(path)
        
        def read(data):
            self.assertEqual(content, data)
        
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        d = self.sftp.write_file(path, content)
        d.addCallback(file_created)
        d.addCallback(read)
        return d

    @with_reactor
    def test_write_read_binary(self):
        content = "0x00 \n \r \r\n \n\r 0xFF"
        
        def file_created(_):
            return self.sftp.read_file(path)
        
        def read(data):
            self.assertEqual(content, data)
        
        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        d = self.sftp.write_file(path, content)
        d.addCallback(file_created)
        d.addCallback(read)
        return d
    
    @with_reactor
    def test_remove_file(self):
        def file_created(_):
            return self.sftp.delete_file(path)

        path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        d = self.sftp.write_file(path, "to be deleted")
        d.addCallback(file_created)
        return d
    
    @with_reactor
    def test_listdir(self):
        
        def directory_filled(_):
            items = self.sftp.list_directory(directory)
            return items
        
        def got_listing(items):
            items = set(items)
            
            expected = set((('file1', False),
             ('file2', False),
             ('dir1', True),
             ('dir2', True)))
            
            self.assertEqual(expected, items)
            
        directory = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
        file1 = directory + "/file1"
        file2 = directory + "/file2"
        dir1 = directory + "/dir1"
        dir2 = directory + "/dir2"
        d = self.sftp.create_directory(directory)
        
        d.addCallback(lambda _: self.sftp.write_file(file1, "file1"))
        d.addCallback(lambda _: self.sftp.write_file(file2, "file2"))
        d.addCallback(lambda _: self.sftp.create_directory(dir1))
        d.addCallback(lambda _: self.sftp.create_directory(dir2))
        d.addCallback(directory_filled)
        d.addCallback(got_listing)
        return d
    

class TestSFTPDirect(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        test_credentials.load_credentials()
        
    @with_reactor
    def test_close(self):
        d = ssh.sftp(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        d.addCallback(lambda sftp:sftp.close())
        return d
    
    @with_reactor
    def test_read_write(self):
        
        def opened(sftp):
            def file_created(_):
                return sftp.read_file(path)
        
            def read(data):
                self.assertEqual("Hello World!", data)
            
            path = test_credentials.ssh_tmp_dir + "/" + str(uuid.uuid4())
            d = sftp.write_file(path, "Hello World!")
            d.addCallback(file_created)
            d.addCallback(read)
            d.addCallback(lambda _:sftp.close())
            return d
        
        d = ssh.sftp(test_credentials.ssh_hostname, 
                           test_credentials.ssh_user, 
                           private_key_files=[test_credentials.ssh_privatekey])
        d.addCallback(opened)
        return d
    
