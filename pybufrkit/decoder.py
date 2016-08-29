from __future__ import absolute_import
from .utils import BpclError, bins_from_string, peek_edition, format_decoded_data
from .vm import VM, minmax, NUMERIC_MISSING_VALUES, NBITS_FOR_NBITS_DIFF
from .bufr import Bufr
from six.moves import range

__all__ = ['Decoder']


# noinspection PyUnusedLocal,PyAttributeOutsideInit
class Decoder(VM):
    def __init__(self,
                 definitions_dir=None,
                 definition_filename=None,
                 tables_root_dir=None,
                 cache_compiled_template=True,
                 compiled_template_dir=None,
                 save_compiled_template=False):

        super(Decoder, self).__init__(VM.MODE_DECODER,
                                      definitions_dir,
                                      definition_filename if definition_filename else 'boot-decode.bpcl',
                                      tables_root_dir,
                                      cache_compiled_template,
                                      compiled_template_dir,
                                      save_compiled_template)

        # These functions are used both from the BPCL files and this file
        self.read_uint = self.gns['read_uint']
        self.read_int = self.gns['read_int']
        self.read_string = self.gns['read_string']

    def decode(self, s, file_path='<string>'):
        """
        Entry point for the whole decoding process. It does its job by executing
        the code object from the definition files. The code object in turns
        calls back to other methods of this class.

        :param bytes s: The bytes string of the BUFR message
        :param file_path: The file path that the input bytes string is read from
        """
        self.decode_preflight(s, file_path)
        # TODO: error handling for code object or let it bubble up to caller?
        exec(self.boot_code, self.gns)

        # Now build and return the BUFR object
        return Bufr(
            input_file_path=self.gns.get('_input_file_path'),
            table_group_info_string=str(self.get('table_group')),
            section0=self.gns.get('section0', None),
            section1=self.gns.get('section1', None),
            section2=self.gns.get('section2', None),
            section3=self.gns.get('section3', None),
            section4=self.gns.get('section4', None),
            section5=self.gns.get('section5', None),
            n_subsets=self.get('n_subsets'),
            is_compressed=self.get('is_compressed'),
            template=self.get('template'),
            unexpanded_descriptors=self.get('unexpanded_descriptors'),
            decoded_descriptors_all_subsets=self.get('decoded_descriptors_all_subsets'),
            decoded_values_all_subsets=self.get('decoded_values_all_subsets'),
            bitmap_links_all_subsets=self.get('bitmap_links_all_subsets'),
        )

    def dumps(self):
        """
        Dumps all working variables of the object. This is useful to debug the
        process in case of error.
        """
        ret = []
        for k, v in self.gns.items():
            if k == '__builtins__' or callable(v):
                continue
            ret.append('{} = {!r}'.format(k, v))

        try:
            ret.extend(format_decoded_data(self.n_subsets,
                                           self.decoded_descriptors_all_subsets,
                                           self.decoded_values_all_subsets,
                                           self.bitmap_links_all_subsets))
        except AttributeError:
            pass

        return '\n'.join(ret)

    # noinspection PyAttributeOutsideInit
    def decode_preflight(self, s, file_path):
        VM._process_preflight(self)
        # Set up new variables
        bins = bins_from_string(s)
        self.gns.update(
            (
                ('_input_file_path', file_path),
                ('_bins', bins),
                ('_edition', peek_edition(bins)),
            )
        )

    # noinspection PyUnresolvedReferences
    def decode_s4_data(self, n_subsets, is_compressed):
        self.decoded_values_all_subsets = [[] for _ in range(n_subsets)]
        VM._process_s4_data_preflight(self, n_subsets, is_compressed)

        func = VM._get_s4_process_func(self)
        func(self, is_decode=True, is_compressed=self.is_compressed)

    def read_uint_or_none(self, nbits):
        value = self.read_uint(nbits)
        if nbits > 1 and value == NUMERIC_MISSING_VALUES[nbits]:
            value = None
        return value

    def get_delayed_replication_factor_value(self):
        return VM._get_delayed_replication_factor_value(self, -1)

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
            bitmap = self.decoded_values_all_subsets[0][-n_031031:]
        else:
            bitmap = self.decoded_values[-n_031031:]
        if reuse:
            self.bitmap = bitmap

        VM._build_bitmapped_descriptors(self, bitmap)
        return bitmap

    def _decode_bitmapped_descriptor(self, *args):
        """
        This is a generic DECODING method for both uncompressed and compressed
        data by wrapping the superclass's even more generic method.
        """
        VM._process_bitmapped_descriptor(self, *args)

    def decode_value_for_descriptor_uncompressed(self, value, descriptor):
        self.decoded_descriptors.append(descriptor)
        self.decoded_values.append(value)

    def decode_string_uncompressed(self, descriptor, nbytes):
        """
        Decode a string value of the given number of bytes

        :param descriptor:
        :param nbytes: Number of bytes to read for the string.
        """
        self.decoded_descriptors.append(descriptor)
        self.decoded_values.append(self.read_string(nbytes))

    def decode_numeric_uncompressed(self, descriptor,
                                    nbits, scale_powered, refval):
        self.decoded_descriptors.append(descriptor)
        value = self.read_uint_or_none(nbits)
        if value is not None:
            if refval:
                value += refval
            if scale_powered != 1:
                value /= scale_powered
        self.decoded_values.append(value)

    def decode_numeric_with_new_refval_uncompressed(self, descriptor,
                                                    nbits, scale_powered, refval_factor):
        self.decode_numeric_uncompressed(descriptor, nbits, scale_powered,
                                         self.refval_new[descriptor.id] * refval_factor)

    def decode_codeflag_uncompressed(self, descriptor, nbits):
        """
        Decode a descriptor of code or flag value. A code or flag value does not
        need to scale and refval.
        """
        self.decoded_descriptors.append(descriptor)
        self.decoded_values.append(self.read_uint_or_none(nbits))

    def decode_new_refval_uncompressed(self, descriptor, nbits):
        self.decoded_descriptors.append(descriptor)
        # NOTE read_int NOT read_uint
        self.refval_new[descriptor.id] = value = self.read_int(nbits)
        # TODO: new descriptor type for new refval
        self.decoded_values.append(value)

    def decode_bitmapped_descriptor_uncompressed(self, *args):
        self._decode_bitmapped_descriptor(self.decode_string_uncompressed,
                                          self.decode_codeflag_uncompressed,
                                          self.decode_numeric_uncompressed,
                                          self.decode_numeric_with_new_refval_uncompressed,
                                          *args)

    def decode_value_for_descriptor_compressed(self, value, descriptor):
        self.decoded_descriptors.append(descriptor)
        for decoded_values in self.decoded_values_all_subsets:
            decoded_values.append(value)

    def decode_string_compressed(self, descriptor, nbytes_min_value):
        """
        Decode a single compressed string value.

        :param descriptor:
        :param nbytes_min_value:
        """
        self.decoded_descriptors.append(descriptor)
        min_value = self.read_string(nbytes_min_value)
        nbits_diff = self.read_uint(NBITS_FOR_NBITS_DIFF)

        if nbits_diff:  # non-zero nbits_diff
            assert min_value == b'\0' * nbytes_min_value, (
                '{}: Different string must be compressed'
                ' with empty min value'.format(descriptor))

        # special cases: all missing or all equals
        if min_value is None or nbits_diff == 0:
            assert nbits_diff == 0, ('{}: nbits_diff must be zero for compressed '
                                     'values that are all missing or equal'.format(descriptor))
            for decoded_values in self.decoded_values_all_subsets:
                decoded_values.append(min_value)
        else:
            for decoded_values in self.decoded_values_all_subsets:
                decoded_values.append(self.read_string(nbits_diff))

    def decode_numeric_compressed(self, descriptor,
                                  nbits_min_value, scale_powered, refval):
        self.decoded_descriptors.append(descriptor)
        min_value = self.read_uint_or_none(nbits_min_value)
        nbits_diff = self.read_uint(NBITS_FOR_NBITS_DIFF)

        # special cases: all missing or all equals
        if min_value is None:
            assert nbits_diff == 0, ('{}: nbits_diff must be zero for compressed '
                                     'values that are all missing or equal'.format(descriptor))
            for decoded_values in self.decoded_values_all_subsets:
                decoded_values.append(None)

        elif nbits_diff == 0:
            value = min_value
            if refval:
                value += refval
            if scale_powered != 1:
                value /= scale_powered
            for decoded_values in self.decoded_values_all_subsets:
                decoded_values.append(value)
        else:
            for decoded_values in self.decoded_values_all_subsets:
                diff = self.read_uint_or_none(nbits_diff)
                if diff is None:
                    value = None
                else:
                    value = min_value + diff
                    if refval:
                        value += refval
                    if scale_powered != 1:
                        value /= scale_powered
                decoded_values.append(value)

    def decode_numeric_with_new_refval_compressed(self, descriptor,
                                                  nbits_min_value, scale_powered, refval_factor):
        self.decode_numeric_compressed(descriptor, nbits_min_value, scale_powered,
                                       self.refval_new[descriptor.id] * refval_factor)

    def decode_codeflag_compressed(self, descriptor, nbits_min_value):
        self.decoded_descriptors.append(descriptor)
        min_value = self.read_uint_or_none(nbits_min_value)
        nbits_diff = self.read_uint(NBITS_FOR_NBITS_DIFF)

        # special cases: all missing or all equals
        if min_value is None or nbits_diff == 0:
            assert nbits_diff == 0, ('{}: nbits_diff must be zero for compressed '
                                     'values that are all missing or equal'.format(descriptor))
            for decoded_values in self.decoded_values_all_subsets:
                decoded_values.append(min_value)
        else:
            for decoded_values in self.decoded_values_all_subsets:
                diff = self.read_uint_or_none(nbits_diff)
                if diff is None:
                    value = None
                else:
                    value = min_value + diff
                    # Still need to check for missing values, e.g. 4 bits code with a value of 15
                    # is actually a missing value
                    if descriptor.nbits > 1 and value == NUMERIC_MISSING_VALUES[descriptor.nbits]:
                        value = None
                decoded_values.append(value)

    def decode_new_refval_compressed(self, descriptor, nbits_min_value):
        self.decoded_descriptors.append(descriptor)
        min_value = self.read_int(nbits_min_value)
        nbits_diff = self.read_uint(NBITS_FOR_NBITS_DIFF)

        assert nbits_diff == 0, ('{}: New reference values must be identical '
                                 'for all subsets for compressed data'.format(descriptor))

        for decoded_values in self.decoded_values_all_subsets:
            decoded_values.append(min_value)

        self.refval_new[descriptor.id] = min_value
        # TODO: new descriptor type for new refval

    def decode_bitmapped_descriptor_compressed(self, *args):
        self._decode_bitmapped_descriptor(self.decode_string_compressed,
                                          self.decode_codeflag_compressed,
                                          self.decode_numeric_compressed,
                                          self.decode_numeric_with_new_refval_compressed,
                                          *args)
