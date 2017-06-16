"""
pybufrkit.encoder
~~~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import functools
import json
import logging
import six

from pybufrkit.constants import (BITPOS_START,
                                 NBITS_FOR_NBITS_DIFF,
                                 NBITS_PER_BYTE,
                                 NUMERIC_MISSING_VALUES,
                                 PARAMETER_TYPE_TEMPLATE_DATA,
                                 PARAMETER_TYPE_UNEXPANDED_DESCRIPTORS)
from pybufrkit.errors import PyBufrKitError
from pybufrkit.bitops import get_bit_writer
from pybufrkit.bufr import BufrMessage
from pybufrkit.templatedata import TemplateData
from pybufrkit.descriptors import Descriptor
from pybufrkit.coder import CoderState, Coder
from pybufrkit.templatecompiler import CompiledTemplateManager, process_compiled_template

__all__ = ['Encoder']

log = logging.getLogger(__file__)


def nbits_for_uint(x):
    """
    Given the maximum difference, find out the number of bits needed for this number
    and also considering the missing value (all ones).
    """
    binx = bin(x)[2:]
    nbits = len(binx)
    # If this number has all bits of one, the required length is one bit longer
    # as all 1s is for missing value.
    if binx.count('1') == len(binx):
        nbits += 1

    return nbits


class Encoder(Coder):
    """
    The encoder takes a JSON object or string and encoded it to a BUFR message.

    :param ignore_declared_length: If set, ignore the section_length declared
        in the input JSON message and always calculated it.
    """

    def __init__(self,
                 definitions_dir=None,
                 tables_root_dir=None,
                 ignore_declared_length=True,
                 compiled_template_cache_max=None):

        super(Encoder, self).__init__(definitions_dir, tables_root_dir)
        self.ignore_declared_length = ignore_declared_length

        # Only enable template compilation if cache is requested
        if compiled_template_cache_max is not None:
            self.compiled_template_manager = CompiledTemplateManager(compiled_template_cache_max)
            log.debug('Template compilation enabled with cache size of {}'.format(compiled_template_cache_max))
        else:
            self.compiled_template_manager = None

    def process(self, s, file_path='<string>', wire_template_data=True):
        """
        Entry point for the encoding process. The process encodes a JSON format
        message to BUFR message.

        :param s: A JSON or its string serialized form
        :param file_path: The file path to the JSON file.
        :param wire_template_data: Whether to wire the template data to construct
            a fully hierarchical structure from the flat lists.

        :return: A bitstring object of the encoded message.
        """

        if isinstance(s, (six.binary_type, six.text_type)):
            # TODO: ensure all strings are loaded as plain ascii instead of unicode from JSON
            json_data = json.loads(s, encoding='latin-1')
        else:
            json_data = s

        bit_writer = get_bit_writer()
        bufr_message = BufrMessage(filename=file_path)

        nbits_encoded = 0
        section_index = 0
        # When a section is not present in the json data. The index must not be
        # increase for the section.
        index_offset = 0
        while True:
            section = self.section_configurer.configure_section_with_values(
                bufr_message, section_index, json_data[section_index - index_offset])
            section_index += 1
            if section is None:  # optional section is not present
                index_offset += 1  # This section should not be counted
                continue
            nbits_encoded += self.process_section(bufr_message, bit_writer, section)
            if section.end_of_message:
                break

        # A zero length means the actual length must be calculated
        # Fix is needed for both the message object and the serialized bits
        nbytes_write = bit_writer.get_pos() // NBITS_PER_BYTE
        if bufr_message.length.value == 0 or self.ignore_declared_length:
            bufr_message.length.value = nbytes_write
            section = bufr_message.length.parent
            bit_writer.set_uint(
                bufr_message.length.value,
                bufr_message.length.nbits,
                section.get_metadata(BITPOS_START) + section.get_parameter_offset('length')
            )
        elif bufr_message.length.value != nbytes_write:
            raise PyBufrKitError('Write exceeds declared total length {} by {} bytes'.format(
                bufr_message.length.value, nbytes_write - bufr_message.length.value
            ))

        bufr_message.serialized_bytes = bit_writer.to_bytes()

        if wire_template_data:
            bufr_message.wire()

        return bufr_message

    def process_section(self, bufr_message, bit_writer, section):
        section.set_metadata(BITPOS_START, bit_writer.get_pos())

        for parameter in section:
            if parameter.type == PARAMETER_TYPE_UNEXPANDED_DESCRIPTORS:
                self.process_unexpanded_descriptors(bit_writer, parameter)
            elif parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                self.process_template_data(bufr_message, bit_writer, parameter)
            else:
                bit_writer.write(parameter.value, parameter.type, parameter.nbits)

            log.debug('{} = {!r}'.format(parameter.name, parameter.value))

            # Make available as a property of the overall message object
            if parameter.as_property:
                setattr(bufr_message, parameter.name, parameter)

        nbits_write = (bit_writer.get_pos() - section.get_metadata(BITPOS_START))
        nbytes_write, nbits_residue = nbits_write // NBITS_PER_BYTE, nbits_write % NBITS_PER_BYTE
        # For edition 3 and earlier, ensure each section has an even number of octets.
        # This is done by padding Zeros to the required number of octets.
        if bufr_message.edition.value <= 3:
            if nbytes_write % 2 != 0:
                nbits_padding_for_octet = (NBITS_PER_BYTE - nbits_residue)
            else:
                nbits_padding_for_octet = 0 if nbits_residue == 0 else (2 * NBITS_PER_BYTE - nbits_residue)
        else:  # otherwise just padding to any integer multiple of octet
            nbits_padding_for_octet = 0 if nbits_residue == 0 else (NBITS_PER_BYTE - nbits_residue)

        if nbits_padding_for_octet != 0:
            log.debug('Padding {} bits for complete Octets'.format(nbits_padding_for_octet))
            bit_writer.write_bin('0' * nbits_padding_for_octet)

        if 'section_length' in section:
            nbits_write = bit_writer.get_pos() - section.get_metadata(BITPOS_START)
            # A zero length means the length should be calculated
            if section.section_length.value == 0 or self.ignore_declared_length:
                section.section_length.value = nbits_write // NBITS_PER_BYTE
                bit_writer.set_uint(
                    section.section_length.value,
                    section.section_length.nbits,
                    section.get_metadata(BITPOS_START) + section.get_parameter_offset('section_length')
                )

            else:
                nbits_unwrite = section.section_length.value * NBITS_PER_BYTE - nbits_write
                if nbits_unwrite > 0:
                    log.debug('Padding {} bits to for declared length of the section'.format(nbits_unwrite))
                    bit_writer.skip(nbits_unwrite)
                elif nbits_unwrite < 0:
                    raise PyBufrKitError('Writing exceeds declared section length {} by {} bytes'.format(
                        section.section_length.value, -nbits_unwrite // NBITS_PER_BYTE
                    ))

        return bit_writer.get_pos() - section.get_metadata(BITPOS_START)

    def process_unexpanded_descriptors(self, bit_writer, section_parameter):
        """
        Encode the list of unexpanded descriptors.
        :type bit_writer: bitops.BitWriter
        :type section_parameter: bufr.SectionParameter
        """
        for descriptor_id in section_parameter.value:
            descriptor = Descriptor(descriptor_id)
            bit_writer.write_uint(descriptor.F, 2)
            bit_writer.write_uint(descriptor.X, 6)
            bit_writer.write_uint(descriptor.Y, 8)

    def process_template_data(self, bufr_message, bit_writer, section_parameter):
        """

        :type bufr_message: bufr.BufrMessage
        :param bit_writer:
        :type section_parameter: bufr.SectionParameter
        :return:
        """
        # TODO: Parametrise the "normalize" argument
        bufr_template, table_group = bufr_message.build_template(self.tables_root_dir, normalize=0)

        state = CoderState(bufr_message.is_compressed.value, bufr_message.n_subsets.value, section_parameter.value)

        if self.compiled_template_manager:
            template_to_process = self.compiled_template_manager.get_or_compile(bufr_template, table_group)
            template_processing_func = functools.partial(process_compiled_template, self)
        else:
            template_to_process = bufr_template
            template_processing_func = self.process_template

        if bufr_message.is_compressed.value:
            template_processing_func(state, bit_writer, template_to_process)
        else:
            for idx_subset in range(bufr_message.n_subsets.value):
                state.switch_subset_context(idx_subset)
                state.idx_value = 0
                template_processing_func(state, bit_writer, template_to_process)

        section_parameter.value = TemplateData(bufr_template,
                                               bufr_message.is_compressed.value,
                                               state.decoded_descriptors_all_subsets,
                                               state.decoded_values_all_subsets,
                                               state.bitmap_links_all_subsets)

    def get_value_for_delayed_replication_factor(self, state):
        return state.get_value_for_delayed_replication_factor(state.idx_value - 1)

    # noinspection PyAttributeOutsideInit
    def define_bitmap(self, state, reuse):
        """
        For compressed data, bitmap and back referenced descriptors must be
        identical Otherwise it makes no sense in compressing different bitmapped
        descriptors into one slot.

        :type state: CoderState
        :param reuse: Is this bitmap for reuse?
        """
        # First get all the bit values for the bitmap
        if state.is_compressed:
            bitmap = state.decoded_values_all_subsets[0][state.idx_value - state.n_031031: state.idx_value]
        else:
            bitmap = state.decoded_values[state.idx_value - state.n_031031: state.idx_value]
        if reuse:  # save the bitmap if it is defined for reuse
            state.bitmap = bitmap

        state.build_bitmapped_descriptors(bitmap)
        return bitmap

    def process_numeric(self, state, bit_writer, descriptor, nbits, scale_powered, refval):
        (self.process_numeric_compressed if state.is_compressed else
         self.process_numeric_uncompressed)(state, bit_writer, descriptor, nbits, scale_powered, refval)

    def process_numeric_uncompressed(self, state, bit_writer, descriptor, nbits, scale_powered, refval):
        state.decoded_descriptors.append(descriptor)
        value = state.decoded_values[state.idx_value]
        state.idx_value += 1
        if value is not None:
            if scale_powered != 1:
                value = int(round(value * scale_powered))
            if refval:
                value -= refval
        else:
            value = NUMERIC_MISSING_VALUES[nbits]
        bit_writer.write_uint(value, nbits)

    def process_numeric_compressed(self, state, bit_writer, descriptor, nbits_min_value, scale_powered, refval):
        values, all_equal, all_missing = self._next_compressed_values_and_status_from_all_subsets(state, descriptor)

        if all_missing:
            min_value = NUMERIC_MISSING_VALUES[nbits_min_value]
            nbits_diff = 0

        elif all_equal:
            min_value = values[0]
            if min_value is None:
                min_value = NUMERIC_MISSING_VALUES[nbits_min_value]
            else:
                if scale_powered != 1:
                    min_value = int(round(min_value * scale_powered))
                if refval:
                    min_value -= refval
            nbits_diff = 0

        else:
            # Apply scale and refval first to the values
            for idx, value in enumerate(values):
                if value is not None:
                    if scale_powered != 1:
                        value = int(round(value * scale_powered))
                    if refval:
                        value -= refval
                values[idx] = value

            min_value, max_value = state.minmax(values)
            nbits_diff = nbits_for_uint(max_value - min_value + 1)
            # Now subtract the minimum from the values
            for idx, value in enumerate(values):
                if value is None:
                    value = NUMERIC_MISSING_VALUES[nbits_diff]
                else:
                    value -= min_value
                values[idx] = value

        bit_writer.write_uint(min_value, nbits_min_value)
        bit_writer.write_uint(nbits_diff, NBITS_FOR_NBITS_DIFF)

        if nbits_diff:
            for value in values:
                bit_writer.write_uint(value, nbits_diff)

    def process_string(self, state, bit_writer, descriptor, nbytes):
        (self.process_string_compressed if state.is_compressed else
         self.process_string_uncompressed)(state, bit_writer, descriptor, nbytes)

    def process_string_uncompressed(self, state, bit_writer, descriptor, nbytes):
        """
        Decode a string value of the given number of bytes

        :param descriptor:
        :param nbytes: Number of bytes to read for the string.
        """
        state.decoded_descriptors.append(descriptor)
        value = state.decoded_values[state.idx_value]
        if value is None:
            value = '\xff' * nbytes
        state.idx_value += 1
        bit_writer.write_bytes(value, nbytes)

    def process_string_compressed(self, state, bit_writer, descriptor, nbytes_min_value):
        values, all_equal, all_missing = self._next_compressed_values_and_status_from_all_subsets(state, descriptor)

        if all_missing:
            min_value = '\xff' * nbytes_min_value
            nbytes_diff = 0
        elif all_equal:
            min_value = values[0]
            nbytes_diff = 0
        else:
            min_value = '\0' * nbytes_min_value
            nbytes_diff = nbytes_min_value

        bit_writer.write_bytes(min_value, nbytes_min_value)
        bit_writer.write_uint(nbytes_diff, NBITS_FOR_NBITS_DIFF)

        if nbytes_diff:
            for value in values:
                if value is None:
                    value = '\xff' * nbytes_diff
                bit_writer.write_bytes(value, nbytes_diff)

    def process_codeflag(self, state, bit_writer, descriptor, nbits):
        (self.process_codeflag_compressed if state.is_compressed else
         self.process_codeflag_uncompressed)(state, bit_writer, descriptor, nbits)

    def process_codeflag_uncompressed(self, state, bit_writer, descriptor, nbits):
        """
        Decode a descriptor of code or flag value. A code or flag value does not
        need to scale and refval.
        """
        state.decoded_descriptors.append(descriptor)
        value = state.decoded_values[state.idx_value]
        state.idx_value += 1
        if value is None:
            value = NUMERIC_MISSING_VALUES[nbits]
        bit_writer.write_uint(value, nbits)

    def process_codeflag_compressed(self, state, bit_writer, descriptor, nbits_min_value):
        values, all_equal, all_missing = self._next_compressed_values_and_status_from_all_subsets(state, descriptor)

        if all_missing:
            min_value = NUMERIC_MISSING_VALUES[nbits_min_value]
            nbits_diff = 0
        elif all_equal:
            min_value = values[0]
            nbits_diff = 0
        else:
            min_value, max_value = state.minmax(values)
            nbits_diff = nbits_for_uint(max_value - min_value + 1)
            # Subtract the minimum from the values
            for idx, value in enumerate(values):
                if value is None:
                    value = NUMERIC_MISSING_VALUES[nbits_diff]
                else:
                    value -= min_value
                values[idx] = value

        bit_writer.write_uint(min_value, nbits_min_value)
        bit_writer.write_uint(nbits_diff, NBITS_FOR_NBITS_DIFF)

        if nbits_diff:
            for value in values:
                bit_writer.write_uint(value, nbits_diff)

    def process_new_refval(self, state, bit_writer, descriptor, nbits):
        (self.process_new_refval_compressed if state.is_compressed else
         self.process_new_refval_uncompressed)(state, bit_writer, descriptor, nbits)

    def process_new_refval_uncompressed(self, state, bit_writer, descriptor, nbits):
        state.decoded_descriptors.append(descriptor)
        state.new_refvals[descriptor.id] = value = state.decoded_values[state.idx_value]
        state.idx_value += 1
        # TODO: new descriptor type for new reference value?
        assert value is not None, \
            '{}: New reference value cannot be missing'.format(descriptor)
        bit_writer.write_int(value, nbits)  # NOTE write_int NOT write_uint

    def process_new_refval_compressed(self, state, bit_writer, descriptor, nbits_min_value):
        values, all_equal, all_missing = self._next_compressed_values_and_status_from_all_subsets(state, descriptor)
        assert all_equal, ('{}: New reference values must be identical '
                           'for all subsets for compressed data'.format(descriptor))
        assert all_missing is False, (
            '{}: New reference value cannot be missing'.format(descriptor))
        min_value = state.new_refvals[descriptor.id] = values[0]
        nbits_diff = 0

        bit_writer.write_int(min_value, nbits_min_value)
        bit_writer.write_uint(nbits_diff, NBITS_FOR_NBITS_DIFF)

        if nbits_diff:
            for value in values:
                bit_writer.write_uint(value - min_value, nbits_diff)

    def process_numeric_with_new_refval(self, state, bit_writer,
                                        descriptor, nbits, scale_powered,
                                        refval_factor):
        """
        Encode a descriptor of numeric value that has a new reference value set
        by 203 YYY. This new reference value must be retrieved at runtime as it
        is defined in the data section.

        :param int refval_factor: The refval factor set as part of 207 YYY
        :return:
        """
        self.process_numeric(state, bit_writer, descriptor, nbits, scale_powered,
                             state.new_refvals[descriptor.id] * refval_factor)

    def process_constant_uncompressed(self, state, bit_writer, descriptor, value):
        """
        This is in fact skip the value for encoding. Useful for operator
        descriptor 222000 etc.
        """
        assert value == state.decoded_values[state.idx_value], '{}: Value must be zero'.format(descriptor)
        state.decoded_descriptors.append(descriptor)
        state.idx_value += 1

    def process_constant(self, state, bit_writer, descriptor, value):
        (self.process_constant_compressed if state.is_compressed else
         self.process_constant_uncompressed)(state, bit_writer, descriptor, value)

    def process_constant_compressed(self, state, bit_writer, descriptor, value):
        """
        This method is used for pop out value 0 for 222000, etc.
        """
        values, all_equal, all_missing = self._next_compressed_values_and_status_from_all_subsets(state, descriptor)
        assert all_equal and values[0] == value, '{}: Value for must be 0'.format(descriptor)

    def _next_compressed_values_and_status_from_all_subsets(self, state, descriptor):
        """
        For compressed data, get the next values from all subsets and check
        whether they are all equal or all missing.

        :return: a tuple of three elements, values, whether they are all identical, or all missing
        """
        state.decoded_descriptors.append(descriptor)
        values = [decoded_values[state.idx_value]
                  for decoded_values in state.decoded_values_all_subsets]
        state.idx_value += 1
        all_equal = values.count(values[0]) == state.n_subsets
        all_missing = (values[0] is None) if all_equal else False
        return values, all_equal, all_missing
