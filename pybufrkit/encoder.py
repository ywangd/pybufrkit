from __future__ import absolute_import
import json

import bitstring
import six

from .vm import VM, minmax, NUMERIC_MISSING_VALUES, NBITS_FOR_NBITS_DIFF

__all__ = ['Encoder']


def nbits_for_uint(x):
    """
    Given the maximum difference, find out the number of bits needed for this number
    and also considering the missing value (all ones).
    """
    binx = bin(x)[2:]
    binx_len = len(binx)
    # If this number has all bits of one, the required length is one bit longer
    # as all 1s is for missing value.
    if binx.count('1') == binx_len:
        nbits = binx_len + 1
    else:
        nbits = binx_len

    return nbits


class Encoder(VM):
    def __init__(self,
                 definitions_dir=None,
                 definition_filename=None,
                 tables_root_dir=None,
                 cache_compiled_template=True,
                 compiled_template_dir=None,
                 save_compiled_template=False):

        super(Encoder, self).__init__(VM.MODE_ENCODER,
                                      definitions_dir,
                                      definition_filename if definition_filename else 'boot-encode.bpcl',
                                      tables_root_dir,
                                      cache_compiled_template,
                                      compiled_template_dir,
                                      save_compiled_template)

        # These functions are used both from the BPCL files and this file
        self.write_uint = self.gns['write_uint']
        self.write_int = self.gns['write_int']
        self.write_string = self.gns['write_string']

    def encode(self, s, file_path='<string>'):
        """
        Entry point for the encoding process. The process encodes a JSON format
        message to BUFR message.

        :param s: A JSON or its string serialized form
        :param file_path: The file path to the JSON file.
        :return: A bitstring object of the encoded message.
        """
        self.encode_preflight(s, file_path)
        exec(self.boot_code, self.gns)
        return self.gns['_bins']

    def encode_preflight(self, s, file_path):
        VM._process_preflight(self)
        if isinstance(s, (six.binary_type, six.text_type)):
            # TODO: ensure all strings are loaded as plain ascii instead of unicode from JSON
            data = json.loads(s, encoding='latin-1')
        else:
            data = s
        self.gns.update(
            (
                ('_json_data', data),
                ('_input_file_path', file_path),
                ('_bins', bitstring.BitStream()),
                ('_edition', data[0][2]),
            )
        )

    # noinspection PyUnresolvedReferences,PyAttributeOutsideInit
    def encode_s4_data(self, decoded_values_all_subsets, is_compressed):
        self.decoded_values_all_subsets = decoded_values_all_subsets
        self.idx_value = 0  # Only needed for encoding
        VM._process_s4_data_preflight(self, len(decoded_values_all_subsets), is_compressed)

        func = VM._get_s4_process_func(self)
        func(self, is_decode=False, is_compressed=self.is_compressed)

    # noinspection PyAttributeOutsideInit
    def switch_subset_context(self, idx_subset):
        """
        This function is only useful for uncompressed data.
        """
        VM.switch_subset_context(self, idx_subset)
        # Index to value is only needed for encoder
        self.idx_value = 0

    def get_delayed_replication_factor_value(self):
        return VM._get_delayed_replication_factor_value(self, self.idx_value - 1)

    # noinspection PyAttributeOutsideInit
    def define_bitmap(self, n_031031, reuse):
        """
        For compressed data, bitmap and back referenced descriptors must be
        identical Otherwise it makes no sense in compressing different bitmapped
        descriptors into one slot.

        :param n_031031: Number of 031031 descriptors used to define this bitmap
        :param reuse: Is this bitmap for reuse?
        """
        # First get all the bit values for the bitmap
        if self.is_compressed:
            bitmap = self.decoded_values_all_subsets[0][self.idx_value - n_031031: self.idx_value]
        else:
            bitmap = self.decoded_values[self.idx_value - n_031031: self.idx_value]
        if reuse:
            self.bitmap = bitmap

        VM._build_bitmapped_descriptors(self, bitmap)
        return bitmap

    def _encode_bitmapped_descriptor(self, *args):
        """
        This is a generic ENCODING method for both uncompressed and compressed
        data by wrapping the superclass's even more generic method.
        """
        VM._process_bitmapped_descriptor(self, *args)

    def encode_value_for_descriptor_uncompressed(self, value, descriptor):
        """
        This is in fact skip the value for encoding. Useful for operator
        descriptor 222000 etc.
        """
        assert value == self.decoded_values[self.idx_value], (
            '{}: Value must be zero'.format(descriptor))
        self.decoded_descriptors.append(descriptor)
        self.idx_value += 1

    def encode_string_uncompressed(self, descriptor, nbytes):
        """
        Decode a string value of the given number of bytes

        :param descriptor:
        :param nbytes: Number of bytes to read for the string.
        """
        self.decoded_descriptors.append(descriptor)
        value = self.decoded_values[self.idx_value]
        if value is None:
            value = '\xff' * nbytes
        self.idx_value += 1
        self.write_string(value, nbytes)

    def encode_numeric_uncompressed(self,
                                    descriptor, nbits, scale_powered,
                                    refval):
        """
        :param descriptor:
        :param nbits:
        :param scale_powered: The scale in 10th power, i.e. 10 ** scale
        :param refval: The reference value
        """
        self.decoded_descriptors.append(descriptor)
        value = self.decoded_values[self.idx_value]
        self.idx_value += 1
        if value is not None:
            if scale_powered != 1:
                value = int(round(value * scale_powered))
            if refval:
                value -= refval
        else:
            value = NUMERIC_MISSING_VALUES[nbits]
        self.write_uint(value, nbits)

    def encode_numeric_with_new_refval_uncompressed(self,
                                                    descriptor, nbits, scale_powered,
                                                    refval_factor):
        """
        Encode a descriptor of numeric value that has a new reference value set
        by 203 YYY. This new reference value must be retrieved at runtime as it
        is defined in the data section.

        :param int refval_factor: The refval factor set as part of 207 YYY
        :return:
        """
        self.encode_numeric_uncompressed(descriptor, nbits, scale_powered,
                                         self.refval_new[descriptor.id] * refval_factor)

    def encode_codeflag_uncompressed(self, descriptor, nbits):
        """
        Decode a descriptor of code or flag value. A code or flag value does not
        need to scale and refval.
        """
        self.decoded_descriptors.append(descriptor)
        value = self.decoded_values[self.idx_value]
        self.idx_value += 1
        if value is None:
            value = NUMERIC_MISSING_VALUES[nbits]
        self.write_uint(value, nbits)

    def encode_new_refval_uncompressed(self, descriptor, nbits):
        self.decoded_descriptors.append(descriptor)
        self.refval_new[descriptor.id] = value = self.decoded_values[self.idx_value]
        self.idx_value += 1
        # TODO: new descriptor type for new refval
        assert value is not None, (
            '{}: New reference value cannot be missing'.format(descriptor))
        self.write_int(value, nbits)  # NOTE write_int NOT write_uint

    def encode_bitmapped_descriptor_uncompressed(self, *args):
        self._encode_bitmapped_descriptor(self.encode_string_uncompressed,
                                          self.encode_codeflag_uncompressed,
                                          self.encode_numeric_uncompressed,
                                          self.encode_numeric_with_new_refval_uncompressed,
                                          *args)

    def _next_compressed_values(self, descriptor):
        """
        For compressed data, get the next values for all subsets and check
        whether they are all equal or all missing.

        :return: a list of values and whether they are identical and all missing
        """
        self.decoded_descriptors.append(descriptor)
        values = [decoded_values[self.idx_value]
                  for decoded_values in self.decoded_values_all_subsets]
        self.idx_value += 1
        all_equal = values.count(values[0]) == self.n_subsets
        all_missing = (values[0] is None) if all_equal else False
        return values, all_equal, all_missing

    def encode_value_for_descriptor_compressed(self, value, descriptor):
        """
        This method is used for pop out value 0 for 222000, etc.
        """
        values, all_equal, all_missing = self._next_compressed_values(descriptor)
        assert all_equal and values[0] == value, '{}: Value for must be 0'.format(descriptor)

    def encode_string_compressed(self, descriptor, nbytes_min_value):
        values, all_equal, all_missing = self._next_compressed_values(descriptor)

        if all_missing:
            min_value = '\xff' * nbytes_min_value
            nbytes_diff = 0
        elif all_equal:
            min_value = values[0]
            nbytes_diff = 0
        else:
            min_value = '\0' * nbytes_min_value
            nbytes_diff = nbytes_min_value

        self.write_string(min_value, nbytes_min_value)
        self.write_uint(nbytes_diff, NBITS_FOR_NBITS_DIFF)

        if nbytes_diff:
            for value in values:
                if value is None:
                    value = '\xff' * nbytes_diff
                self.write_string(value, nbytes_diff)

    def encode_numeric_compressed(self,
                                  descriptor,
                                  nbits_min_value, scale_powered, refval):
        values, all_equal, all_missing = self._next_compressed_values(descriptor)

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

            min_value, max_value = minmax(values)
            nbits_diff = nbits_for_uint(max_value - min_value + 1)
            # Now subtract the minimum from the values
            for idx, value in enumerate(values):
                if value is None:
                    value = NUMERIC_MISSING_VALUES[nbits_diff]
                else:
                    value -= min_value
                values[idx] = value

        self.write_uint(min_value, nbits_min_value)
        self.write_uint(nbits_diff, NBITS_FOR_NBITS_DIFF)

        if nbits_diff:
            for value in values:
                self.write_uint(value, nbits_diff)

    def encode_numeric_with_new_refval_compressed(self,
                                                  descriptor, nbits_min_value, scale_powered,
                                                  refval_factor):
        self.encode_numeric_compressed(descriptor, nbits_min_value, scale_powered,
                                       self.refval_new[descriptor.id] * refval_factor)

    def encode_codeflag_compressed(self, descriptor, nbits_min_value):
        values, all_equal, all_missing = self._next_compressed_values(descriptor)

        if all_missing:
            min_value = NUMERIC_MISSING_VALUES[nbits_min_value]
            nbits_diff = 0
        elif all_equal:
            min_value = values[0]
            nbits_diff = 0
        else:
            min_value, max_value = minmax(values)
            nbits_diff = nbits_for_uint(max_value - min_value + 1)
            # Subtract the minimum from the values
            for idx, value in enumerate(values):
                if value is None:
                    value = NUMERIC_MISSING_VALUES[nbits_diff]
                else:
                    value -= min_value
                values[idx] = value

        self.write_uint(min_value, nbits_min_value)
        self.write_uint(nbits_diff, NBITS_FOR_NBITS_DIFF)

        if nbits_diff:
            for value in values:
                self.write_uint(value, nbits_diff)

    def encode_new_refval_compressed(self, descriptor, nbits_min_value):
        values, all_equal, all_missing = self._next_compressed_values(descriptor)
        assert all_equal, ('{}: New reference values must be identical '
                           'for all subsets for compressed data'.format(descriptor))
        assert all_missing is False, (
            '{}: New reference value cannot be missing'.format(descriptor))
        min_value = self.refval_new[descriptor.id] = values[0]
        nbits_diff = 0

        self.write_int(min_value, nbits_min_value)
        self.write_uint(nbits_diff, NBITS_FOR_NBITS_DIFF)

        if nbits_diff:
            for value in values:
                self.write_uint(value - min_value, nbits_diff)

    def encode_bitmapped_descriptor_compressed(self, *args):
        self._encode_bitmapped_descriptor(self.encode_string_compressed,
                                          self.encode_codeflag_compressed,
                                          self.encode_numeric_compressed,
                                          self.encode_numeric_with_new_refval_compressed,
                                          *args)
