"""
pybufrkit.decoder
~~~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import functools
import logging
# noinspection PyUnresolvedReferences
from six.moves import range

from pybufrkit.constants import (BITPOS_START,
                                 MESSAGE_START_SIGNATURE,
                                 NBITS_FOR_NBITS_DIFF,
                                 NBITS_PER_BYTE,
                                 NUMERIC_MISSING_VALUES,
                                 PARAMETER_TYPE_TEMPLATE_DATA,
                                 PARAMETER_TYPE_UNEXPANDED_DESCRIPTORS)
from pybufrkit.errors import PyBufrKitError
from pybufrkit.bitops import get_bit_reader
from pybufrkit.bufr import BufrMessage
from pybufrkit.templatedata import TemplateData
from pybufrkit.coder import Coder, CoderState
from pybufrkit.templatecompiler import CompiledTemplateManager, process_compiled_template

__all__ = ['Decoder']

log = logging.getLogger(__file__)


# noinspection PyUnusedLocal,PyAttributeOutsideInit
class Decoder(Coder):
    """
    The decoder takes a bytes type string and decode it to a BUFR Message object.
    """

    def __init__(self,
                 definitions_dir=None,
                 tables_root_dir=None,
                 compiled_template_cache_max=None):

        super(Decoder, self).__init__(definitions_dir, tables_root_dir)

        # Only enable template compilation if cache is requested
        if compiled_template_cache_max is not None:
            self.compiled_template_manager = CompiledTemplateManager(compiled_template_cache_max)
            log.debug('Template compilation enabled with cache size of {}'.format(compiled_template_cache_max))
        else:
            self.compiled_template_manager = None

    def process(self, s, file_path='<string>',
                start_signature=MESSAGE_START_SIGNATURE,
                info_only=False,
                ignore_value_expectation=False,
                wire_template_data=True):
        """
        Decoding the given message string.

        :param s: Message string that contains the BUFR Message
        :param file_path: The file where this string is read from.
        :param start_signature: Locate the starting position of the message
            string with the given signature.
        :param info_only: Only show information up to template data (exclusive)
        :param ignore_value_expectation: Do not validate the expected value
        :param wire_template_data: Whether to wire the template data to construct
            a fully hierarchical structure from the flat lists. Only takes effect
            when it is NOT info_only.

        :return: A BufrMessage object that contains the decoded information.
        """
        idx = s.find(start_signature) if start_signature is not None else 0
        if idx == -1:
            raise PyBufrKitError('Cannot find start signature: {}'.format(start_signature))
        s = s[idx:]

        bit_reader = get_bit_reader(s)
        bufr_message = BufrMessage(file_path)

        configuration_transformers = (self.section_configurer.info_configuration,) if info_only else ()
        if ignore_value_expectation:
            configuration_transformers += (self.section_configurer.ignore_value_expectation,)

        nbits_decoded = 0
        section_index = 0  # Always start decoding from section 0
        while True:
            section = self.section_configurer.configure_section(bufr_message, section_index,
                                                                configuration_transformers)
            section_index += 1
            if section is None:  # when optional section is not present
                continue
            nbits_decoded += self.process_section(bufr_message, bit_reader, section)
            if section.end_of_message:
                break

        # The exact bytes that have been decoded
        bufr_message.serialized_bytes = s[:nbits_decoded // NBITS_PER_BYTE]

        if not info_only and wire_template_data:
            bufr_message.wire()

        return bufr_message

    def process_section(self, bufr_message, bit_reader, section):
        """
        Decode the given configured Section.

        :param bufr_message: The BUFR message object.
        :param section: The BUFR section object.
        :param bit_reader:
        :return: Number of bits decoded for this section.
        """
        section.set_metadata(BITPOS_START, bit_reader.get_pos())

        for parameter in section:
            if parameter.type == PARAMETER_TYPE_UNEXPANDED_DESCRIPTORS:
                parameter.value = self.process_unexpanded_descriptors(bit_reader, section)
            elif parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                parameter.value = self.process_template_data(bufr_message, bit_reader)
            elif parameter.nbits == 0:
                # Zero number of bits means to read all bits till the end of the section
                parameter.value = bit_reader.read(
                    parameter.type,
                    section.section_length.value * NBITS_PER_BYTE -
                    (bit_reader.get_pos() - section.get_metadata(BITPOS_START))
                )
            else:
                parameter.value = bit_reader.read(parameter.type, parameter.nbits)

            log.debug('{} = {!r}'.format(parameter.name, parameter.value))

            # Make available as a property of the overall message object
            if parameter.as_property:
                setattr(bufr_message, parameter.name, parameter)

            if parameter.expected is not None:
                assert parameter.value == parameter.expected, 'Value ({!r}) not as expected ({!r})'.format(
                    parameter.value, parameter.expected
                )

        # TODO: option to ignore the declared length?
        # TODO: this depends on a specific parameter name, need change to parameter type?
        if 'section_length' in section:
            nbits_read = bit_reader.get_pos() - section.get_metadata(BITPOS_START)
            nbits_unread = section.section_length.value * NBITS_PER_BYTE - nbits_read
            if nbits_unread > 0:
                log.debug('Skipping {} bits to end of the section'.format(nbits_unread))
                bit_reader.read_bin(nbits_unread)
            elif nbits_unread < 0:
                raise PyBufrKitError('Read exceeds declared section length: {}'.format(section.section_length.value))

        return bit_reader.get_pos() - section.get_metadata(BITPOS_START)

    def process_unexpanded_descriptors(self, bit_reader, section):
        """
        Decode for the list of unexpanded descriptors.

        :param section: The BUFR section object.
        :param bit_reader:
        :return: The unexpanded descriptors as a list.
        """
        unexpanded_descriptors = []
        nbytes_read = (bit_reader.get_pos() - section.get_metadata(BITPOS_START)) // NBITS_PER_BYTE
        for _ in range((section.section_length.value - nbytes_read) // 2):
            f = bit_reader.read_uint(2)
            x = bit_reader.read_uint(6)
            y = bit_reader.read_uint(8)
            unexpanded_descriptors.append(f * 100000 + x * 1000 + y)

        return unexpanded_descriptors

    def process_template_data(self, bufr_message, bit_reader):
        """
        Decode data described by the template.

        :param bufr_message: The BUFR message object.
        :param bit_reader:
        :return: TemplateData decoded from the bit stream.
        """
        # TODO: Parametrise the "normalize" argument
        bufr_template, table_group = bufr_message.build_template(self.tables_root_dir, normalize=1)

        state = CoderState(bufr_message.is_compressed.value, bufr_message.n_subsets.value)

        if self.compiled_template_manager:
            template_to_process = self.compiled_template_manager.get_or_compile(bufr_template, table_group)
            template_processing_func = functools.partial(process_compiled_template, self)
        else:
            template_to_process = bufr_template
            template_processing_func = self.process_template

        # For uncompressed data, the processing has to be repeated for number of times
        # equals to number of subsets. For compressed data, only a single processing
        # is needed as all subsets are taken care each time a value is processed.
        if bufr_message.is_compressed.value:
            template_processing_func(state, bit_reader, template_to_process)
        else:
            for idx_subset in range(bufr_message.n_subsets.value):
                state.switch_subset_context(idx_subset)
                template_processing_func(state, bit_reader, template_to_process)

        return TemplateData(bufr_template,
                            bufr_message.is_compressed.value,
                            state.decoded_descriptors_all_subsets,
                            state.decoded_values_all_subsets,
                            state.bitmap_links_all_subsets)

    def get_value_for_delayed_replication_factor(self, state):
        return state.get_value_for_delayed_replication_factor(-1)

    def define_bitmap(self, state, reuse):
        """
        For compressed data, bitmap and back referenced descriptors must be
        identical Otherwise it makes no sense in compressing different bitmapped
        descriptors into one slot.

        :param state:
        :param reuse: Is this bitmap for reuse?
        :return: The bitmap as a list of 0 and 1.
        """
        # First get all the bit values for the bitmap
        if state.is_compressed:
            bitmap = state.decoded_values_all_subsets[0][-state.n_031031:]
        else:
            bitmap = state.decoded_values[-state.n_031031:]
        if reuse:
            state.bitmap = bitmap

        state.build_bitmapped_descriptors(bitmap)
        return bitmap

    def process_numeric(self, state, bit_reader, descriptor, nbits, scale_powered, refval):
        (self.process_numeric_compressed if state.is_compressed else
         self.process_numeric_uncompressed)(state, bit_reader, descriptor, nbits, scale_powered, refval)

    def process_numeric_uncompressed(self, state, bit_reader, descriptor, nbits, scale_powered, refval):
        state.decoded_descriptors.append(descriptor)
        value = bit_reader.read_uint_or_none(nbits)
        if value is not None:
            if refval:
                value += refval
            if scale_powered != 1:
                value /= scale_powered
        state.decoded_values.append(value)

    def process_numeric_compressed(self, state, bit_reader, descriptor, nbits_min_value, scale_powered, refval):
        state.decoded_descriptors.append(descriptor)
        min_value = bit_reader.read_uint_or_none(nbits_min_value)
        nbits_diff = bit_reader.read_uint(NBITS_FOR_NBITS_DIFF)

        # special cases: all missing or all equals
        if min_value is None:
            assert nbits_diff == 0, ('{}: nbits_diff must be zero for compressed '
                                     'values that are all missing or equal'.format(descriptor))
            for decoded_values in state.decoded_values_all_subsets:
                decoded_values.append(None)

        elif nbits_diff == 0:
            value = min_value
            if refval:
                value += refval
            if scale_powered != 1:
                value /= scale_powered
            for decoded_values in state.decoded_values_all_subsets:
                decoded_values.append(value)
        else:
            for decoded_values in state.decoded_values_all_subsets:
                diff = bit_reader.read_uint_or_none(nbits_diff)
                if diff is None:
                    value = None
                else:
                    value = min_value + diff
                    if refval:
                        value += refval
                    if scale_powered != 1:
                        value /= scale_powered
                decoded_values.append(value)

    def process_string(self, state, bit_reader, descriptor, nbytes):
        (self.process_string_compressed if state.is_compressed else
         self.process_string_uncompressed)(state, bit_reader, descriptor, nbytes)

    def process_string_uncompressed(self, state, bit_reader, descriptor, nbytes):
        state.decoded_descriptors.append(descriptor)
        state.decoded_values.append(bit_reader.read_bytes(nbytes))

    def process_string_compressed(self, state, bit_reader, descriptor, nbytes_min_value):
        state.decoded_descriptors.append(descriptor)
        min_value = bit_reader.read_bytes(nbytes_min_value)
        nbits_diff = bit_reader.read_uint(NBITS_FOR_NBITS_DIFF)

        if nbits_diff:  # non-zero nbits_diff
            assert min_value == b'\0' * nbytes_min_value, (
                '{}: Different string must be compressed'
                ' with empty min value'.format(descriptor))

        # special cases: all missing or all equals
        if min_value is None or nbits_diff == 0:
            assert nbits_diff == 0, ('{}: nbits_diff must be zero for compressed '
                                     'values that are all missing or equal'.format(descriptor))
            for decoded_values in state.decoded_values_all_subsets:
                decoded_values.append(min_value)
        else:
            for decoded_values in state.decoded_values_all_subsets:
                decoded_values.append(bit_reader.read_bytes(nbits_diff))

    def process_codeflag(self, state, bit_reader, descriptor, nbits):
        (self.process_codeflag_compressed if state.is_compressed else
         self.process_codeflag_uncompressed)(state, bit_reader, descriptor, nbits)

    def process_codeflag_uncompressed(self, state, bit_reader, descriptor, nbits):
        state.decoded_descriptors.append(descriptor)
        state.decoded_values.append(bit_reader.read_uint_or_none(nbits))

    def process_codeflag_compressed(self, state, bit_reader, descriptor, nbits_min_value):
        state.decoded_descriptors.append(descriptor)
        min_value = bit_reader.read_uint_or_none(nbits_min_value)
        nbits_diff = bit_reader.read_uint(NBITS_FOR_NBITS_DIFF)

        # special cases: all missing or all equals
        if min_value is None or nbits_diff == 0:
            assert nbits_diff == 0, ('{}: nbits_diff must be zero for compressed '
                                     'values that are all missing or equal'.format(descriptor))
            for decoded_values in state.decoded_values_all_subsets:
                decoded_values.append(min_value)
        else:
            for decoded_values in state.decoded_values_all_subsets:
                diff = bit_reader.read_uint_or_none(nbits_diff)
                if diff is None:
                    value = None
                else:
                    value = min_value + diff
                    # Still need to check for missing values, e.g. 4 bits code with a value of 15
                    # is actually a missing value
                    if descriptor.nbits > 1 and value == NUMERIC_MISSING_VALUES[descriptor.nbits]:
                        value = None
                decoded_values.append(value)

    def process_new_refval(self, state, bit_reader, descriptor, nbits):
        (self.process_new_refval_compressed if state.is_compressed else
         self.process_new_refval_uncompressed)(state, bit_reader, descriptor, nbits)

    def process_new_refval_uncompressed(self, state, bit_reader, descriptor, nbits):
        state.decoded_descriptors.append(descriptor)
        # NOTE read_int NOT read_uint
        state.new_refvals[descriptor.id] = value = bit_reader.read_int(nbits)
        # TODO: new descriptor type for new refval
        state.decoded_values.append(value)

    def process_new_refval_compressed(self, state, bit_reader, descriptor, nbits_min_value):
        state.decoded_descriptors.append(descriptor)
        min_value = bit_reader.read_int(nbits_min_value)
        nbits_diff = bit_reader.read_uint(NBITS_FOR_NBITS_DIFF)

        assert nbits_diff == 0, ('{}: New reference values must be identical '
                                 'for all subsets for compressed data'.format(descriptor))

        for decoded_values in state.decoded_values_all_subsets:
            decoded_values.append(min_value)

        state.new_refvals[descriptor.id] = min_value
        # TODO: new descriptor type for new refval

    # TODO: this method can be removed if we don't use compiled template.
    def process_numeric_of_new_refval(self, state, bit_reader,
                                      descriptor, nbits, scale_powered,
                                      refval_factor):
        self.process_numeric(state, bit_reader, descriptor, nbits, scale_powered,
                             state.new_refvals[descriptor.id] * refval_factor)

    def process_constant(self, state, bit_reader, descriptor, value):
        (self.process_constant_compressed if state.is_compressed else
         self.process_constant_uncompressed)(state, bit_reader, descriptor, value)

    def process_constant_uncompressed(self, state, bit_reader, descriptor, value):
        state.decoded_descriptors.append(descriptor)
        state.decoded_values.append(value)

    def process_constant_compressed(self, state, bit_reader, descriptor, value):
        state.decoded_descriptors.append(descriptor)
        for decoded_values in state.decoded_values_all_subsets:
            decoded_values.append(value)


def generate_bufr_message(decoder, s, info_only=False, *args, **kwargs):
    """
    This is a generator function that processes the given string for one
    or more BufrMessage till it is exhausted.

    :param Decoder decoder: Decoder to use
    :param bytes s: String to decode for messages
    :return: BufrMessage object
    """
    idx_start = 0
    while idx_start < len(s):
        idx_start = s.find(MESSAGE_START_SIGNATURE, idx_start)
        if idx_start < 0:
            raise StopIteration
        bufr_message = decoder.process(
            s[idx_start:], start_signature=None, info_only=info_only, *args, **kwargs
        )
        # If data section is not decoded, we rely on the declared length for the message length
        if info_only:
            bufr_message.serialized_bytes = s[idx_start: idx_start + bufr_message.length.value]
        idx_start += len(bufr_message.serialized_bytes)
        yield bufr_message
