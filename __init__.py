## -*- coding: UTF-8 -*-
## __init__.py
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
from io import BytesIO, TextIOWrapper, BufferedReader
from collections import OrderedDict
from construct.lib import Container
from datetime import datetime
from functools import wraps

from .common.task import BaseTask
from .common.patterns import RegistryMetaclassMixin
from .utils import FileMetadataMixin, StructureProperty

class ParserMeta(RegistryMetaclassMixin, type):
    '''
    Meta class for creating parsers that processes
    the StructureProperty attributes on the class definition
    and does some other prepatory work.
    '''
    @classmethod
    def _create_class(cls, name, bases, attrs):
        '''
        @RegistryMetaclassMixin._create_class
        '''
        attrs['_PROPERTIES'] = OrderedDict()
        properties  = attrs.get('_PROPERTIES')
        current_idx = 0
        for base in bases:
            if hasattr(base, '_PROPERTIES'):
                for prop in base._PROPERTIES:
                    properties[prop] = base._PROPERTIES.get(prop)
                    properties.get(prop).idx = current_idx
                    current_idx += 1
                    new_property = property(
                        properties.get(prop).get_property,
                        properties.get(prop).set_property
                    )
                    attrs[prop] = new_property
        for key, value in sorted(
            [(key, value) for key, value in attrs.items() if isinstance(value, StructureProperty)], 
            key=lambda pair: pair[1].idx
        ):
            new_property = property(value.get_property, value.set_property)
            properties[key] = value
            properties.get(key).idx = current_idx
            current_idx += 1
            attrs[key] = new_property
        return super()._create_class(name, bases, attrs)
    @classmethod
    def _add_class(cls, name, new_cls):
        pass

def contexted(*args):
    '''
    Args:
        close: Boolean              => whether to exit the context after 
                                       executing the function
        OR
        func: Function<Any> -> Any  => function to wrap around
    Returns:
        Function<Any> -> Any
        Decorator that mimics a with statement for a single function
    Preconditions:
        This function is used as a decorator on an instance method
        of a class that implements the context manager interface
    '''
    if len(args) == 0:
        raise TypeError('No argument or function supplied to decorator')
    else:
        close = None
        func = None
        if isinstance(args[0], bool):
            close = args[0]
        elif callable(args[0]):
            close = False
            func = args[0]
        else:
            raise TypeError('Decorator expects either a Boolean or function, received %s'%type(args[0]).__name__)
    def outer_wrapper(f):
        @wraps(f)
        def inner_wrapper(self, *args, **kwargs):
            if hasattr(self, '__enter__') and callable(self.__enter__) and \
                hasattr(self, '__exit__') and callable(self.__exit__):
                self.__enter__()
                try:
                    return f(self, *args, **kwargs)
                finally:
                    if close:
                        self.__exit__()
            else:
                raise TypeError(
                    '%s does not implement the context manager interface'%type(self).__name__
                )
        return inner_wrapper 
    if func is None:
        return outer_wrapper
    else:
        return outer_wrapper(func)

class BaseParser(BaseTask, metaclass=ParserMeta):
    '''
    Base class for creating parsers
    '''
    def __init__(self, *args, stream=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream = stream
    @property
    def stream(self):
        '''
        Getter for stream
        '''
        return self.__stream
    @stream.setter
    def stream(self, value):
        '''
        Setter for stream
        '''
        assert value is None or isinstance(value, (TextIOWrapper, BufferedReader, BytesIO))
        self.__stream = value
    def _clean_value(self, value, serialize=False):
        '''
        Args:
            value: Any          => value to be converted
            serialize: Boolean  => transform values recursively to be JSON-serializable
        Returns:
            Any
            Raw value if it is not of type Container, else recursively removes
            any key beginning with 'Raw'
        Preconditions:
            serialize is of type Boolean
        '''
        assert isinstance(serialize, bool)
        if issubclass(type(value), Container):
            cleaned_value = Container(value)
            for key in cleaned_value:
                if key.startswith('Raw') or key.startswith('_'):
                    del cleaned_value[key]
                else:
                    cleaned_value[key] = self._clean_value(cleaned_value[key], serialize)
            return cleaned_value
        elif isinstance(value, list):
            return list(map(lambda entry: self._clean_value(entry, serialize), value))
        elif isinstance(value, datetime) and serialize:
            return value.strftime('%Y-%m-%d %H:%M:%S.%f%z')
        else:
            return value
    def create_stream(self, persist=False):
        '''
        Args:
            persist: Boolean    => whether to persist stream as attribute on self
        Returns:
            New stream for this parser
        Preconditions:
            persist of type Boolean
        '''
        raise NotImplementedError('create_stream is not implemented for type %s'%(type(self).__name__))
    def _parse_continue(self, structure, result):
        '''
        Args:
            structure: String   => structure parsed
            result: Any         => result from parsing property
        Procedure:
            Determine whether the parser should continue
        Preconditions:
            structure is of type String
            result is of type Any
        '''
        assert isinstance(structure, str)
        return True
    def __enter__(self):
        '''
        Args:
            N/A
        Procedure:
            Set self.stream if it is not already set.  Acts as the 
            entry point for context manager ("with ... as fhandle").
        Preconditions:
            N/A
        '''
        if self.stream is None:
            self.create_stream(persist=True)
    def __exit__(self):
        '''
        Args:
            N/A
        Procedure:
            Close self.stream if it is set.  Acts as the 
            exit point for context manager ("with ... as fhandle").
        Preconditions:
            N/A
        '''
        if self.stream is not None:
            self.stream.close()
            self.stream = None
    def _preamble(self):
        '''
        @BaseTask._preamble
        '''
        super()._preamble()
        self.__enter__()
    def _postamble(self):
        '''
        @BaseTask._postamble
        '''
        super()._postamble()
        self.__exit__()
    @contexted
    def parse_structure(self, structure, *args, **kwargs):
        '''
        Args:
            structure: String   => the structure to parse
        Returns
            Dict<String, Any>
            Result(s) from parsing the structure if known, raise ValueError otherwise
        Preconditions:
            structure is of type String
        '''
        assert isinstance(structure, str)
        if not (structure in self._PROPERTIES):
            raise ValueError('%s is not a valid structure'%structure)
        parser = '_parse_%s'%structure
        if not hasattr(self, parser):
            raise ValueError('no parser implemented for structure %s'%structure)
        deps = self._PROPERTIES.get(structure).deps
        if deps is not None:
            for dep in deps:
                prop = self._PROPERTIES.get(dep)
                if prop is None:
                    raise ValueError('found invalid dependency %s for structure %s'%(dep, structure))
                elif prop.dynamic:
                    raise ValueError('found dynamic dependency %s for structure %s'%(dep, structure))
                elif getattr(self, dep) is None:
                    try:
                        setattr(self, dep, self.parse_structure(dep, *args, **kwargs))
                    except Exception as e:
                        raise ValueError('failed to parse dependency %s of structure %s (%s)'%(
                            dep,
                            structure,
                            str(e)
                        ))
        for kwarg in kwargs:
            if kwarg != structure and kwarg in self._PROPERTIES:
                kwargs[kwarg] = getattr(self, kwarg)
        return getattr(self, parser)(*args, **kwargs)
    def _process_task(self):
        '''
        Args:
            N/A
        Procedure:
            Attempt to parse the stream based on
            the properites defined on the parser
        Preconditions:
            N/A
        '''
        try:
            for prop in self._PROPERTIES.values():
                if not prop.dynamic:
                    try:
                        result = self.parse_structure(prop.name)
                        setattr(self, prop.name, result)
                    except Exception as e:
                        Logger.error('Failed to parse structure %s (%s)'%(prop.name, str(e)))
                        result = dict(prop=prop, err=e)
                    if not self._parse_continue(prop.name, result):
                        break
            return self
        except:
            pass
    def parse(self):
        '''
        @BaseTask.run
        '''
        self.run()
        return self

class ByteParser(BaseParser):
    '''
    Class for parsing byte streams
    '''
    def __init__(self, source, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = source
    @property
    def source(self):
        '''
        Getter for source
        '''
        return self.__source
    @source.setter
    def source(self, value):
        '''
        Setter for source
        '''
        assert isinstance(value, (bytes, bytearray))
        self.__source = value
    def create_stream(self, persist=False):
        '''
        @BaseParser.create_stream
        '''
        assert isinstance(persist, bool)
        stream = BytesIO(self.source)
        if persist:
            self.stream = stream
        return stream

class FileParser(FileMetadataMixin, BaseParser):
    '''
    Class for parsing file streams
    '''
    def __init__(self, source, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = source
    @property
    def source(self):
        '''
        Getter for source
        '''
        return self.__source
    @source.setter
    def source(self, value):
        '''
        Setter for source
        '''
        assert isinstance(value, str)
        self.__source = value
    def create_stream(self, persist=False):
        '''
        @BaseParser.create_stream
        '''
        assert isinstance(persist, bool)
        stream = open(self.source, 'rb')
        if persist:
            self.stream = stream
        return stream
