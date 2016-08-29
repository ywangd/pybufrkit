"""
Functions that can be accessed by users BPCL source files. Functions with names
start with bpcl_decoder are exported for Decoder mode. Functions with names
start with bpcl_encoder are exported for Encoder mode. Functions with names
start with just bpcl_ are exported for both modes.

The exported functions have names with the leading bpcl_, bpcl_decoder_, and
bpcl_encoder prefixes removed. BPCL scripts cannot have variable names start
with an underscore. Therefore all exposed underscore functions are used for
keywords, e.g. section.
"""
from __future__ import absolute_import
from __future__ import print_function
import contextlib

import bitstring

from .utils import BpclNamespace, BpclError
from .tables import get_table_group
import six
from six.moves import range


# TODO: Allow functions to be provided by users

def bpcl_decoder_read_string(self, nbytes):
    """

    :type self: pybufrkit.decoder.Decoder
    """
    bins = self.gns['_bins']
    return bins.read('bytes:{}'.format(nbytes))


def bpcl_decoder_read_uint(self, nbits):
    """

    :type self: pybufrkit.decoder.Decoder
    """
    bins = self.gns['_bins']
    fmt_string = ('uintbe:{}' if nbits % 8 == 0 else 'uint:{}').format(nbits)
    return bins.read(fmt_string)


def bpcl_decoder_read_int(self, nbits):
    bins = self.gns['_bins']
    fmt_string = ('intbe:{}' if nbits % 8 == 0 else 'int:{}').format(nbits)
    return bins.read(fmt_string)


def bpcl_decoder_read_bool(self):
    """

    :type self: pybufrkit.decoder.Decoder
    """
    bins = self.gns['_bins']
    return bins.read('bool')


def bpcl_decoder_read_bin(self, nbits):
    bins = self.gns['_bins']
    return bins.read('bin:{}'.format(nbits))


def bpcl_decoder_read_bits(self, nbits):
    bins = self.gns['_bins']
    return bins.read('bits:{}'.format(nbits))


def bpcl_decoder_peek_uint(self, nbits):
    bins = self.gns['_bins']
    fmt_string = ('uintbe:{}' if nbits % 8 == 0 else 'uint:{}').format(nbits)
    return bins.peek(fmt_string)


@contextlib.contextmanager
def bpcl_decoder__section(self, section_index):
    """
    Note double underscore so that the function name will be _section in
    BPCL builtins. This is to ensure this function can only be invoked by
    the section keyword NOT by calling it directly.

    :param section_index: Index of a section, 0 - 5
    :type self: pybufrkit.decoder.Decoder
    """
    gns_count_start = len(self.gns)
    bins = self.gns['_bins']
    self.gns['_section{}_start'.format(section_index)] = nbits_start = bins.pos

    if section_index == 0:
        section_length = 8
    elif section_index == 5:
        section_length = 4
    else:  # peek length of the section
        section_length = bpcl_decoder_peek_uint(self, 24)
    # Save the length so it can be safely referred later on
    self.gns['_section{}_length'.format(section_index)] = section_length

    yield

    nbits_read = bins.pos - nbits_start
    # Always read to the end of the section
    # Read to the declared length
    # noinspection PyUnboundLocalVariable
    nbits_unread = section_length * 8 - nbits_read

    if nbits_unread < 0:
        raise BpclError('Read over section boundary: {} extra bits'.format(-nbits_unread))
    elif nbits_unread > 0:
        bpcl_decoder_read_bin(self, nbits_unread)

    # Assign variable to the section namespace
    ns = BpclNamespace()
    for k in list(self.gns.keys())[gns_count_start:]:
        if not k.startswith('_'):
            ns[k] = self.gns[k]

    self.gns['section{}'.format(section_index)] = ns


def bpcl_load_bufr_tables(self,
                          master_table_number,
                          originating_centre,
                          originating_subcentre,
                          master_table_version,
                          local_table_version,
                          normalize):
    # TODO: The user bpcl script may set it to any variable or even discard it
    # So we must set the table group as an attribute to the decoder to
    # preserve it for future use.
    self.table_group = get_table_group(self.gns['_tables_root_dir'],
                                       master_table_number,
                                       originating_centre,
                                       originating_subcentre,
                                       master_table_version,
                                       local_table_version,
                                       bool(normalize))
    return self.table_group


def bpcl_decoder_read_unexpanded_descriptors(self):
    """

    :type self: pybufrkit.decoder.Decoder
    """
    unexpanded_descriptors = []
    nbytes_read = (self.gns['_bins'].pos - self.gns['_section3_start']) // 8
    for _ in range((self.gns['_section3_length'] - nbytes_read) // 2):
        f = self.read_uint(2)
        x = self.read_uint(6)
        y = self.read_uint(8)
        unexpanded_descriptors.append(f * 100000 + x * 1000 + y)

    # TODO: save it as an attribute for future reference
    self.unexpanded_descriptors = unexpanded_descriptors

    # print unexpanded_descriptors
    return unexpanded_descriptors


def bpcl_expand_descriptors(self, unexpanded_descriptors):
    template = self.table_group.template_from_ids(*unexpanded_descriptors)
    return template


def bpcl__template_build(self):
    self.template = self.table_group.template_from_ids(*self.unexpanded_descriptors)


def bpcl__template_dump(self):
    print(self.template.dumps())


def bpcl_decoder__load(self, bufr_filename):
    """
    Open a BUFR file.
    """
    with open(bufr_filename, 'rb') as ins:
        s = ins.read()

    self.decode_preflight(s, bufr_filename)


def bpcl_decoder_decode_data_stream(self, n_subsets, is_compressed):
    """
    Decode the binary data stream in section 4
    :param pybufrkit.decoder.Decoder self:
    :param n_subsets:
    :param is_compressed:
    :return:
    """
    self.decode_s4_data(n_subsets, is_compressed)


def bpcl_encoder_skip(self, nbits):
    self.gns['_bins'] += ('uintbe:{}={}' if nbits % 8 == 0 else
                          'uint:{}={}').format(nbits, 0)


def bpcl_encoder_write_string(self, value, nbytes=None):
    if nbytes is None:
        nbytes = len(value)
    # TODO: strings are utf-8 from json reading
    if isinstance(value, six.text_type):
        value = value.encode('latin-1')
    # Ensure the string is under the required data width
    value_len = len(value)
    if value_len > nbytes:
        value = value[:nbytes]
    elif value_len < nbytes:
        value += b' ' * (nbytes - value_len)

    self.gns['_bins'] += bitstring.Bits(bytes=value)
    return value


def bpcl_encoder_write_uint(self, value, nbits):
    value = int(value)
    self.gns['_bins'] += ('uintbe:{}={}' if nbits % 8 == 0 else
                          'uint:{}={}').format(nbits, value)
    return value


def bpcl_encoder_write_int(self, value, nbits):
    value = int(value)
    self.gns['_bins'] += ('intbe:{}={}' if nbits % 8 == 0 else
                          'int:{}={}').format(nbits, value)
    return value


def bpcl_encoder_write_bool(self, value):
    self.gns['_bins'] += 'bool={}'.format(value)
    return value


def bpcl_encoder_write_bin(self, value):
    """

    :param self:
    :param value: Binary number in string format, e.g. '10100'
    :return:
    """
    nbits = len(value)
    self.gns['_bins'] += 'bin:{}={}'.format(nbits, value)
    return value


@contextlib.contextmanager
def bpcl_encoder__section(self, section_index):
    # TODO: better way to find out whether section 2 is present?
    if section_index == 0:
        self.gns['_encoded_sections'] = [0]
    else:
        self.gns['_encoded_sections'].append(section_index)

    self.gns['$'] = self.gns['_json_data'][len(self.gns['_encoded_sections']) - 1]
    bins = self.gns['_bins']
    self.gns['_section{}_start'.format(section_index)] = nbits_start = bins.len

    yield

    nbits_write = bins.len - nbits_start

    # Ensure complete octect
    if nbits_write % 8 != 0:
        nbits_unwrite = 8 - nbits_write % 8
        bpcl_encoder_write_bin(self, '0' * nbits_unwrite)

    # For edition 3 and lower, ensure even number of octets for each section
    if self.gns['_edition'] <= 3:
        if ((bins.len - nbits_start) // 8) % 2 != 0:
            bpcl_encoder_write_bin(self, '00000000')

    # Update section length
    if section_index in (1, 2, 3, 4):
        bins[nbits_start: nbits_start + 24] = bitstring.Bits(
            uintbe=(bins.len - nbits_start) // 8, length=24)

    # Update total length
    bins[32: 56] = bitstring.Bits(uintbe=bins.len // 8, length=24)


def bpcl_encoder_write_unexpanded_descriptors(self, unexpanded_descriptors):
    self.unexpanded_descriptors = unexpanded_descriptors
    for descriptor_id in unexpanded_descriptors:
        descriptor_id = int(descriptor_id)
        y = descriptor_id % 1000
        x = descriptor_id // 1000 % 100
        f = descriptor_id // 100000

        bpcl_encoder_write_uint(self, f, 2)
        bpcl_encoder_write_uint(self, x, 6)
        bpcl_encoder_write_uint(self, y, 8)


def bpcl_encoder_encode_data_stream(self, decoded_values_all_subsets, is_compressed):
    self.encode_s4_data(decoded_values_all_subsets, is_compressed)


# Following functions are not part of BPCL builtins
# So they do not use globals()


_locals = locals()


def collect_bpcl_builtins():
    ret = {
        k: v for k, v in _locals.items()
        if not k.startswith('_') and k.startswith('bpcl_')
        }
    return ret


if __name__ == '__main__':
    print(list(collect_bpcl_builtins().keys()))
