## -*- coding: UTF-8 -*-
## utils.py
##
## Copyright (c) 2018 analyzeDFIR
## 
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
## 
## The above copyright notice and this permission notice shall be included in all
## copies or substantial portions of the Software.
## 
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.

import logging
Logger = logging.getLogger(__name__)
from os import path
import hashlib
from datetime import datetime
from dateutil.tz import tzlocal, tzutc

class FileMetadataMixin(object):
    '''
    Mixin class to provide functions for retrieving file metadata (including hashes).
    '''
    @staticmethod
    def __hash_file(filepath, algorithm):
        '''
        Args:
            filepath: String    => path to file to hash
            algorithm: String   => hash algorithm to use
        Returns:
            String
            Hex digest of hash of prefetch file
        Preconditions:
            filepath is of type String
            algorithm is of type String
        '''
        assert isinstance(filepath, str)
        assert isinstance(algorithm, str)
        try:
            hash = getattr(hashlib, algorithm)
        except Exception as e:
            Logger.error('Unable to obtain %s hash of file (%s)'%(algorithm, str(e)))
            return None
        else:
            hashfile = open(filepath, 'rb')
            try:
                chunk = hashfile.read(4096)
                while chunk != b'':
                    hash.update(chunk)
                return hash.hexdigest()
            finally:
                hashfile.close()
                hashfile = None
    @classmethod
    def __get_metadata(cls, filepath):
        '''
        Args:
            filepath: String    => path to file to get metadata for
        Returns:
            Dict<String, Any>
            Metadata of the target file:
                file_name: file name
                file_path: full path on local system
                file_size: size of file on local system
                md5hash: MD5 hash of file
                sha1hash: SHA1 hash of file
                sha2hash: SHA256 hash of file
                modify_time: last modification time of file on local system (UTC)
                access_time: last access time of file on local system (UTC)
                create_time: create time of file on local system (UTC)
        Preconditions:
            filepath is not None and points to a valid file (assumed True)
        '''
        try:
            return dict(
                file_name=path.basename(filepath),
                file_path=path.abspath(filepath),
                file_size=path.getsize(filepath),
                md5hash=cls.__hash_file(filepath, 'md5'),
                sha1hash=cls.__hash_file(filepath, 'sha1'),
                sha2hash=cls.__hash_file(filepath, 'sha256'),
                modify_time=datetime.fromtimestamp(
                    path.getmtime(filepath), 
                    tzlocal()
                ).astimezone(tzutc()),
                access_time=datetime.fromtimestamp(
                    path.getatime(filepath), 
                    tzlocal()
                ).astimezone(tzutc()),
                create_time=datetime.fromtimestamp(
                    path.getctime(filepath), 
                    tzlocal()
                ).astimezone(tzutc())
            )
        except Exception as e:
            Logger.error('Failed to retrieve metadata for file %s (%s)'%(filepath, str(e)))

    @property
    def metadata(self):
        '''
        Getter for metadata
        '''
        if not hasattr(self.source) or not path.exists(self.source):
            return None
        elif not hasattr(self.__metadatadata) or self.__metadatadata is None:
            self.__metadatadata = self.__get_metadatadata(self.source)
        return self.__metadatadata
    @metadata.setter
    def metadata(self, value):
        '''
        Setter for metadata
        '''
        raise AttributeError('Cannot set dynamic attribute metadata')

class StructureProperty(object):
    '''
    Wrapper class that allows the creation of parser properties
    in a declarative fashion.  For example, if writing a parser
    for MFT entries then the parser definition would look like:
    class MFTEntry(BaseParser):
        header              = StructureProperty(1, 'header')
        file_info           = StructureProperty(2, 'file_info', deps=['header'])
        file_metrics        = StructureProperty(3, 'file_metrics', deps=['header', 'file_info'])
        ...
        directory_strings   = StructureProperty(
            6
            'directory_strings', 
            deps=['file_info', 'volumes_info']
        )
    '''
    def __init__(self, idx, name, deps=None, read_only=False):
        self.idx = idx
        self.name = name
        self.deps = deps
        self.read_only = read_only
    @property
    def idx(self):
        '''
        Getter for idx
        '''
        return self.__idx
    @idx.setter
    def idx(self, value):
        '''
        Setter for idx
        '''
        assert isinstance(value, int)
        self.__idx = value
    @property
    def name(self):
        '''
        Getter for name
        '''
        return self.__name
    @name.setter
    def name(self, value):
        '''
        Setter for name
        '''
        assert isinstance(value, str)
        self.__name = value
    @property
    def deps(self):
        '''
        Getter for deps
        '''
        return self.__deps
    @deps.setter
    def deps(self, value):
        '''
        Setter for deps
        '''
        assert value is None or isinstance(value, list)
        self.__deps = value
    @property
    def read_only(self):
        '''
        Getter for read_only
        '''
        return self.__read_only
    @read_only.setter
    def read_only(self, value):
        '''
        Setter for read_only
        '''
        assert isinstance(value, bool)
        self.__read_only = value
    def _check_dependencies(self, obj):
        '''
        Args:
            obj: Any    => object or class to check dependencies against
        Returns:
            True if obj has the dependencies set for this property, False otherwise
        Preconditions:
            obj is an instance of a class or Class
        '''
        if self.deps is None:
            return True
        return all([(hasattr(obj, dep) and getattr(obj, dep) is not None) for dep in self.deps if dep != self.name])
    def get_property(self, obj):
        '''
        Args:
            obj: Any    => object or class to check dependencies against
        Returns:
            name of obj if the property is retrievable, otherwise
            raises AttributeError with the proper error message
        Preconditions:
            obj is an instance of a class or Class
        '''
        prop = '__%s'%self.name
        if obj is None:
            raise AttributeError('no object to retrieve property from')
        elif not (hasattr(obj, '_PROPERTIES') and self.name in obj._PROPERTIES):
            raise AttributeError('object of type %s does not have property %s'%(type(obj), self.name))
        elif not self._check_dependencies(obj):
            raise AttributeError('dependencies not met for property %s (%s)'%(self.name, ', '.join(self.deps)))
        elif not hasattr(obj, prop):
            return None
        return getattr(obj, prop)
    def set_property(self, obj, value):
        '''
        Args:
            obj: Any    => object or class to check dependencies against
        Procedure:
            Attempt to set the name on obj to value if the property is not
            ready only, otherwise raise an AttributeError
        Preconditions:
            obj is an instance of a class or Class
        '''
        if self.read_only:
            raise AttributeError('property %s is read-only'%self.name)
        try:
            setattr(obj, '__%s'%self.name, value)
        except Exception as e:
            raise AttributeError('failed to set value on property %s (%s)'%(self.name, str(e)))
    def __repr__(self):
        return '%s(%s)'%(
            type(self).__name__,
            ', '.join([
                repr(self.idx),
                "'" + self.name + "'",
                'deps=%s'%repr(self.deps),
                'read_only=%s'%repr(self.read_only)
            ])
        )
