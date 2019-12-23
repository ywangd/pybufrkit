"""
pybufrkit.bitops
~~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import abc
import six

from pybufrkit.constants import (NBITS_PER_BYTE,
                                 NUMERIC_MISSING_VALUES)
from pybufrkit.errors import BitReadError


class BitReader(object):
    @abc.abstractmethod
    def get_pos(self):
        """Retrieve the bit position for next read"""

    def read(self, data_type, nbits):
        """Read nbits for value of given type"""
        func = getattr(self, 'read_' + data_type)
        if data_type == 'bytes':
            return func(nbits // NBITS_PER_BYTE)
        elif data_type == 'bool':
            return func()
        else:
            return func(nbits)

    @abc.abstractmethod
    def read_bytes(self, nbytes):
        """Read number of bytes for value of bytes type"""

    @abc.abstractmethod
    def read_uint(self, nbits):
        """Read number of bites for value of unsigned integer"""

    @abc.abstractmethod
    def read_bool(self):
        """Read one bit for value of boolean"""

    @abc.abstractmethod
    def read_bin(self, nbits):
        """Read number of bits as bytes representation of binary number"""

    @abc.abstractmethod
    def read_int(self, nbits):
        """Read number of bits as integer"""

    def read_uint_or_none(self, nbits):
        value = self.read_uint(nbits)
        if nbits > 1 and value == NUMERIC_MISSING_VALUES[nbits]:
            value = None
        return value


class BitWriter(object):
    @abc.abstractmethod
    def get_pos(self):
        """Retrieve the bit position for next write"""

    @abc.abstractmethod
    def to_bytes(self):
        """dump all content to bytes type"""

    @abc.abstractmethod
    def skip(self, nbits):
        """Skip ahead for the given number of nbits"""

    def write(self, value, data_type, nbits):
        """Write given number of bits for the value of given data type"""
        func = getattr(self, 'write_' + data_type)
        if data_type == 'bytes':
            return func(value, nbits // NBITS_PER_BYTE)
        elif data_type in ('bool', 'bin'):
            return func(value)
        else:
            return func(value, nbits)

    @abc.abstractmethod
    def write_bytes(self, value, nbytes=None):
        """Write given number of bits value of bytes type. If nbytes is none,
        use the length of the given bytes value"""

    @abc.abstractmethod
    def write_uint(self, value, nbits):
        """Write given number of bits value of unsigned integer type"""

    @abc.abstractmethod
    def write_int(self, value, nbits):
        """Write given number of bits for value of signed integer"""

    @abc.abstractmethod
    def write_bool(self, value):
        """Write one bit for value of boolean type"""

    @abc.abstractmethod
    def write_bin(self, value):
        """
        Write a binary number represented by the given value. The length
        is determined by the value.
        """

    @abc.abstractmethod
    def set_uint(self, value, nbits, bitpos):
        """
        Set an unsigned integer value of given number of bits at the bit position
        and replace the old value.
        """


class BitStringBitReader(BitReader):
    """
    A BitReader implementation using the bitstring module.

    :param bit_stream bitstring.BitStream: Bit stream created from the input string
    """

    def __init__(self, s):
        import bitstring
        self.bit_stream = bitstring.BitStream(bytes=s)
        self.bitstring_Error = bitstring.Error

    def get_pos(self):
        return self.bit_stream.pos

    def _bit_stream_read(self, fmt_string):
        """
        This wrapper method is mainly used to wrap the bitstring error type as a
        PyBufrkitError type.

        :param str fmt_string: The format string used to read the bits
        :return: Value of requested type.
        """
        try:
            return self.bit_stream.read(fmt_string)
        except self.bitstring_Error as e:
            raise BitReadError(e.msg)

    def read_bytes(self, nbytes):
        return self._bit_stream_read('bytes:{}'.format(nbytes))

    def read_uint(self, nbits):
        fmt_string = ('uintbe:{}' if nbits % NBITS_PER_BYTE == 0 else 'uint:{}').format(nbits)
        return self._bit_stream_read(fmt_string)

    def read_bool(self):
        return self._bit_stream_read('bool')

    def read_bin(self, nbits):
        return self._bit_stream_read('bin:{}'.format(nbits))

    def read_int(self, nbits):
        return (-1 if self.read_bool() else 1) * self.read_uint(nbits - 1)


class BitStringBitWriter(BitWriter):
    """
    A BitWriter implementation using the bitstring module.
    """

    def __init__(self):
        import bitstring
        self.bit_stream = bitstring.BitStream()

    def get_pos(self):
        return self.bit_stream.len

    def to_bytes(self):
        return self.bit_stream.bytes

    def skip(self, nbits):
        self.bit_stream += ('uintbe:{}={}' if nbits % NBITS_PER_BYTE == 0 else
                            'uint:{}={}').format(nbits, 0)

    def write_bytes(self, value, nbytes=None):
        import bitstring
        # TODO: strings are utf-8 from json reading
        if isinstance(value, six.text_type):
            value = value.encode('latin-1')

        value_len = len(value)

        # Ensure the string is under the required data width
        if nbytes is None:
            nbytes = value_len
        else:
            if value_len > nbytes:
                value = value[:nbytes]
            elif value_len < nbytes:
                value += b' ' * (nbytes - value_len)

        # Cannot use string format shortcut, i.e. 'bytes:{}={}' due to the
        # automatic whitespace trimming by bitstring.
        self.bit_stream += bitstring.Bits(bytes=value)
        return value

    def write_uint(self, value, nbits):
        value = int(value)
        self.bit_stream += ('uintbe:{}={}' if nbits % NBITS_PER_BYTE == 0 else
                            'uint:{}={}').format(nbits, value)
        return value

    def write_int(self, value, nbits):
        value = int(value)
        self.write_bool(value < 0)
        self.write_uint(abs(value), nbits - 1)
        return value

    def write_bool(self, value):
        self.bit_stream += 'bool={}'.format(value)
        return value

    def write_bin(self, value):
        nbits = len(value)
        self.bit_stream += 'bin:{}={}'.format(nbits, value)
        return value

    def set_uint(self, value, nbits, bitpos):
        import bitstring
        if nbits // NBITS_PER_BYTE == 0:
            bins = bitstring.Bits(uint=value, length=nbits)
        else:
            bins = bitstring.Bits(uintbe=value, length=24)
        self.bit_stream[bitpos: bitpos + nbits] = bins


def get_bit_reader(s):
    """
    Initialise and return a BitReader the given string. This function is
    intended to shield the actual implementation of BitReader away from
    the caller.

    :param s: The byte string to read from.
    :return: BitReader
    """
    return BitStringBitReader(s)


def get_bit_writer():
    """
    Initialise and return a BitWriter.

    :return: BitWriter
    """
    return BitStringBitWriter()
