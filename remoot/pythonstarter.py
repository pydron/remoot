# Copyright (C) 2014 Stefan C. Mueller

"""
Starts Python interpreters on this or other machines.

The API is the same as for :mod:`starter`. Only that instead of a command you pass
a python script.
"""

import os
import sys
import subprocess
from remoot import starter

class LocalStarter(object):
    
    def __init__(self):
        self._starter = starter.LocalStarter()
        self.kill = _kill_by_exit
    
    def start(self, script, fileset={}):
        return self._starter.start([sys.executable, "-c", script], fileset)
        
        
class SSHStarter(object):
    
    
    def __init__(self, hostname, username, password=None, private_key_files=[], private_keys=[], tmp_dir="/tmp"):
        self._starter = starter.SSHStarter(hostname, username, password, private_key_files, private_keys, tmp_dir)
        self.kill = _kill_by_exit
    
    def start(self, script, fileset={}):
        return self._starter.start(["/usr/bin/env", "python", "-c", script], fileset)
        
        
class EC2Starter(object):
    
    
    def __init__(self, username, provider, provider_keyid, provider_key, image_id, size_id, public_key_file, private_key_file, tmp_dir):
        self._starter = starter.EC2Starter(username, provider, provider_keyid, provider_key, image_id, size_id, public_key_file, private_key_file, tmp_dir)
        self.kill = _kill_by_shutdown
        
    def start(self, script, fileset={}):
        return self._starter.start(["/usr/bin/env", "python", "-c", script], fileset)
        

def _kill_by_exit():
    """
    Exits the python interpreter without any cleanup.
    This isn't a member function because we need to pass it to the boot stript
    in smartstarter.
    """
    os._exit(0)
        
def _kill_by_shutdown():
    """
    Shuts down the machine.
    This isn't a member function because we need to pass it to the boot stript
    in smartstarter.
    """
    subprocess.call(["/sbin/shutdown", "-h", "now"])
    os._exit(0)
    