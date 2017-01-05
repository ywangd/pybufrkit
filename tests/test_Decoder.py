from __future__ import absolute_import
from __future__ import print_function

import os
import unittest
import functools

from six import PY3, binary_type, text_type
# noinspection PyUnresolvedReferences
from six.moves import range

from pybufrkit.decoder import Decoder

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


class DecoderTests(unittest.TestCase):
    def setUp(self):
        self.decoder = Decoder()
        self.filename_stubs = [
            'IUSK73_AMMC_182300',
            'rado_250',  # uncompressed with 222000, 224000, 236000
            '207003',  # compressed with delayed replication
            'amv2_87',  # compressed with 222000
            'b005_89',  # compressed with 222000 and 224000 (1st order stats)
            'profiler_european',  # uncompressed with 204001 associated fields
            'jaso_214',  # compressed with 204001 associated fields
            'uegabe',  # uncompressed with 204004 associated fields
            'asr3_190',  # compressed with complex replication and 222000, 224000
            'b002_95',  # uncompressed with skipped local descriptors
            'g2nd_208',  # compressed with identical string values for all subsets
            'ISMD01_OKPR',  # compressed with different string values for subsets
            'mpco_217',
        ]

    def tearDown(self):
        pass

    def _compare(self, bufr_message, cmp_file_name):
        with open(os.path.join(DATA_DIR, cmp_file_name)) as ins:
            lines = ins.readlines()

        next_line = functools.partial(next, iter(lines))
        for idx_subset in range(len(bufr_message.template_data.value.decoded_values_all_subsets)):
            for idx, value in enumerate(bufr_message.template_data.value.decoded_values_all_subsets[idx_subset]):
                cmp_line = next_line().strip()
                if value is None:
                    line = '{} {}'.format(idx + 1, repr(value))
                    assert line == cmp_line, \
                        'At line {}: {} != {}'.format(idx + 1, line, cmp_line)
                elif isinstance(value, (binary_type, text_type)):
                    # TODO: better to decode all ascii bytes to unicode string
                    if isinstance(value, binary_type) and PY3:
                        line = '{} {}'.format(idx + 1, repr(value)[1:])
                    else:
                        line = '{} {}'.format(idx + 1, repr(value))
                    assert line == cmp_line, \
                        'At line {}: {} != {}'.format(idx + 1, line, cmp_line)
                else:
                    field = cmp_line.split()[1]
                    if field.endswith('L'):
                        field = field[:-1]
                    cmp_value = eval(field)
                    assert abs(value - cmp_value) < 1.0e6, \
                        'At line {}: {} != {}'.format(idx + 1, value, cmp_value)

    def _print_values(self, bufr_message):
        for idx_subset in range(len(bufr_message.template_data.value.decoded_values_all_subsets)):
            for idx, value in enumerate(bufr_message.template_data.value.decoded_values_all_subsets[idx_subset]):
                print(idx + 1, repr(value))

    def do_test(self, filename_stub):
        s = read_bufr_file(filename_stub + '.bufr')
        bufr_message = self.decoder.process(s, filename_stub)
        self._compare(bufr_message, filename_stub + '.values.cmp')

    def test_decode(self):
        print()
        for filename_stub in self.filename_stubs:
            print(filename_stub)
            self.do_test(filename_stub)
