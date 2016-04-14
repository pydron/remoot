# Copyright (C) 2014 Stefan C. Mueller

"""
Helper methods to create a zip file of packages and to add such a file to the
search path.
"""

import StringIO as stringio
import zipfile
import itertools
import pkgutil
import sys
import inspect
import logging
import os.path
import threading
from twisted.internet import defer, threads

logger = logging.getLogger(__name__)


class _CacheHolder(object):
    
    def __init__(self, names):
        self.names = names
        self.lock = threading.RLock()
        self.waiting_ds = []
        self.zip_content = None
    
    def get(self):
        with self.lock:
            if self.zip_content is None:
                d = defer.Deferred()
                self.waiting_ds.append(d)
            else:
                d = defer.succeed(self.zip_content)
            return d
        
    def set(self, content):
        with self.lock:
            self.zip_content = content
        for d in self.waiting_ds:
            d.callback(content)
            
    def __repr__(self):
        content = "available" if self.zip_content else "building"
        return "_CacheHolder(%r, %r)" % (self.names, content)
    
    
zipfile_cache = {}
    
def cached_make_package_zip(packages):
    """
    Same as `make_package_zip` but uses a background thread and caches
    the result.
    Returns a deferred.
    """
    names = tuple(p.__name__ for p in packages)
    if names in zipfile_cache:
        holder = zipfile_cache[names]
    else:
        holder = _CacheHolder(names)
        zipfile_cache[names] = holder
        
        def make():
            content = make_package_zip(packages)
            return content
            
        def setholder(content):
            holder.set(content)
        
        d = threads.deferToThread(make)
        d.addCallback(setholder)

    return holder.get()
    

def make_package_zip(packages):
    """
    :param packages: List of packages to include in the zip file.
    
    :returns: ZIP-file content as a string.
    """
          
    def _add_package(zf, package):
        path = package.__path__
        prefix = package.__name__ + "."
        for _, modname, ispkg in itertools.chain([(None, package.__name__, True)], pkgutil.walk_packages(path, prefix)):
            if ispkg:
                sourcefile = modname.replace(".", '/') + "/__init__.py"
            else:
                sourcefile = modname.replace(".", '/') + ".py"
    
            try:
                __import__(modname)
            except ImportError as e:
                raise ImportError("Unable to import %s: %r" % (modname, e))
            module = sys.modules[modname]
            src = getsource(module)
            logger.debug("Adding to zip: %r" % sourcefile)
            zf.writestr(sourcefile, src)

    s = stringio.StringIO()
    zf = zipfile.ZipFile(s, 'w', compression=zipfile.ZIP_DEFLATED)
    try:
        for package in set(packages):
            _add_package(zf, package)
    finally:
        zf.close()

    return  s.getvalue()


def getsource(module):
    """
    Lets try to get the source of the module.
    
    We try three different methods. Really, this should be as simple as
    ``inspect.getsource(m)`. Unfortunately there are bugs(*) in there, therefore 
    we try less elegant methods first. 
    
    *) It fails for empty modules (zero-byte source file).
    """
    
    def from_file(module):
        """
        Use  `open(m.__file__).read()`
        """
        if not hasattr(module, "__file__"):
            return None
        
        path = module.__file__
        
        if path.endswith(".pyc"):
            path = path[:-4] + ".py"
        
        if not path.endswith(".py"):
            return None
        
        if not os.path.isfile(path):
            return None
        
        return open(path, 'r').read()
    
    def from_loader(module):
        """
        Use `m.__loader__.get_source(m.__name__)`
        """
        if not hasattr(module, "__loader__"):
            return None
        loader = module.__loader__
        
        if not hasattr(module, "__name__"):
            return None
        name = module.__name__
        
        if not hasattr(loader, "get_source"):
            return None
        
        try:
            return loader.get_source(name)
        except:
            return None
        
    def from_inspect(module):
        """
        Use `inspect.getsource(m)`
        """
        try:
            return inspect.getsource(module)
        except:
            return None
        
    source = from_file(module)
    if source is None:
        source = from_loader(module)
    if source is None:
        source = from_inspect(module)
    
    if source is None:
        raise IOError("Could not get source of module %s (%s)" %(module.__name__, module.__file__))
    else:
        return source
        
def register_zip(filepath):
    """
    Registers a zip file created with :func:`make_package_zip` such that the packages
    can be loaded.
    """
    sys.path.append(filepath)
