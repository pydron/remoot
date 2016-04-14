# Copyright (C) 2014 Stefan C. Mueller

"""
Credentials required to run the integration tests.

For obvious reasons we don't want them to be in the source repository.
So we store them in an ini-file. This module provides easy access
to those credentials.

The variables are defined in the module and set to `None` intially. This
helps with code completion and also gives a location to document them.
"""

#: Hostname or IP to a machine with a bash shell.
#: The machine must have Python installed.
ssh_hostname = None

#: Username that is allowed to log in and run Python.
ssh_user = None

#: Password that matches `ssh_user`. Note that both password and key authentification is required.
ssh_password = None

#: Private key that matches `ssh_user`. Note that both password and key authentification is required.
ssh_privatekey = None

#: Existing directory were temorary files can be put during testing.
ssh_tmp_dir = None


#: Username to login on the started instances
ec2_username = None

#: Which of the EC2 providers to use (defines location)
ec2_provider = None

#: ID of the access key used for authentification
ec2_accesskeyid = None

#: Secret access key used for authentification
ec2_accesskey = None

#: ID of the AMI to boot
ec2_imageid = None

#: ID of the instance size
ec2_sizeid = None

#: public key that matches `ec2_privatekey`. Used to authenticate with the started node.
ec2_publickey = None

#: private key that matches `ec2_publickey`. Used to authenticate with the started node.
ec2_privatekey = None

#: Existing directory on the node were temorary files can be put during testing.
ec2_tmp_dir = None

import os.path
import ConfigParser as configparser


def load_credentials():
    """
    Loads the credentials from `test_credentials.ini`.
    
    It looks at the following locations and takes the first it can find:
    
    * Environment variable `TEST_CREDENTIALS` (must contain the file name).
    
    * Current directory
    
    * Home directory
    
    The ini file should have the a section `[test_credentials]` containing
    the key-value pairs that will be come variables of this module.
    """
    
    if "TEST_CREDENTIALS" in os.environ:
        ini_file = os.environ["TEST_CREDENTIALS"]
    elif os.path.isfile(os.path.abspath("test_credentials.ini")):
        ini_file = os.path.abspath("test_credentials.ini")
    elif os.path.isfile(os.path.expanduser("~/test_credentials.ini")):
        ini_file = os.path.expanduser("~/test_credentials.ini")
    else:
        raise ValueError("Unable to locate test_credentials.ini")
    
    parser = configparser.SafeConfigParser()
    parser.read([ini_file])
    
    if "test_credentials" not in parser.sections():
        raise ValueError("No [test_credentials] section in %s" % ini_file)
    
    for key, value in parser.items("test_credentials"):
        globals()[key] = value
        
