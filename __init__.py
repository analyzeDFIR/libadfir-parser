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
            if isinstance(value, StructureProperty):
                new_property = property(value.get_property, value.set_property)
                properties[key] = value
                properties.get(key).idx = current_idx
                current_idx += 1
                attrs[key] = new_property
        return super()._create_class(name, bases, attrs)
    @classmethod
    def _add_class(cls, name, new_cls):
        pass

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
    def _preamble(self):
        '''
        @BaseTask._preamble
        '''
        super()._preamble()
        if self.stream is None:
            self.create_stream(persist=True)
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
    def _postamble(self):
        '''
        @BaseTask._postamble
        '''
        super()._postamble()
        if self.stream is not None:
            self.stream.close()
            self.stream = None
    def parse_structure(self, structure, *args, stream=None, **kwargs):
        '''
        Args:
            structure: String               => the structure to parse
            stream: TextIOWrapper|BufferedReader|BytesIO   => the stream to parse from
        Returns
            Dict<String, Any>
            Result(s) from parsing the structure if known, raise ValueError otherwise
        Preconditions:
            structure is of type String
            stream is of type TextIOWrapper, BufferedReader or BytesIO
        '''
        assert isinstance(structure, str)
        assert stream is None or isinstance(stream (TextIOWrapper, BufferedReader, BytesIO))
        if not (structure in self._PROPERTIES):
            raise ValueError('%s is not a valid structure'%structure)
        parser = '_parse_%s'%structure
        if not hasattr(self, parser):
            raise ValueError('no parser implemented for structure %s'%structure)
        for kwarg in kwargs:
            if kwarg != structure and kwarg in self._PROPERTIES:
                kwargs[kwarg] = getattr(self, kwarg)
        kwargs['stream'] = stream if stream is not None else self.stream
        if self._PROPERTIES.get(structure).deps is not None:
            for dep in self._PROPERTIES.get(structure).deps:
                if getattr(self, dep) is None:
                    try:
                        setattr(self, dep, self.parse_structure(dep, *args, **kwargs))
                    except Exception as e:
                        raise ValueError('failed to parse dependency %s of structure %s (%s)'%(
                            dep,
                            structure,
                            str(e)
                        ))
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
