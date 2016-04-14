from setuptools import setup, find_packages

import os.path

if os.path.exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = ""

if os.name == "nt":
    os_dependendies = ['pypiwin32>=219']
else:
    os_dependendies = []

setup(
    name = "remoot",
    version = "2.1.4",
    url = "http://www.s-mueller.ch",
    description='Remotely boots Python interpreters and establishes remote invocation capability with them.',
    long_description=long_description,
    author='Stefan C. Mueller',
    author_email='stefan.mueller@fhnw.ch',
    packages = find_packages(),
    package_data={'anypy': ['templates/*.html']},
    setup_requires=['coverage', 'nose>=1.0', 'unittest2'],
    install_requires = ['unittest2>=0.8.0', 
                        'mock>=1.0.1',
                        'pycrypto>=2.6', 
                        'pyasn1>=0.1.7', 
                        'twisted>=15.0.0', 
                        'apache-libcloud>=0.16.0', 
                        'utwist>=0.1.3', 
                        'twistit>=0.2.1', 
                        'anycall>=0.3.0',
                        'sourblossom>=0.1.1'] + os_dependendies,
)
