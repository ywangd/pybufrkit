from __future__ import absolute_import
from __future__ import print_function
import os
import unittest

from pybufrkit.decoder import Decoder
from pybufrkit.bufrdata import BufrData, parse_position_string

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


class BufrDataTests(unittest.TestCase):
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

    def do_test(self, filename_stub):
        s = read_bufr_file(filename_stub + '.bufr')
        bufr = self.decoder.decode(s, filename_stub)
        bufr_data = bufr.wire_data()

        if filename_stub in ('207003', 'rado_250'):
            with open(os.path.join(DATA_DIR,
                                   '{}.datadump.cmp'.format(filename_stub))) as ins:
                cmp_str = ins.read()
            dump_str = bufr_data.dumps()
            # TODO: this is to fix the inconsistent int and long of bitstring on different OS
            dump_str = dump_str.replace(
                '005040 ORBIT NUMBER 5258\n', '005040 ORBIT NUMBER 5258L\n')
            assert dump_str == cmp_str, dump_str
        else:
            bufr_data.dumps()

    def test_bufr_data(self):
        print()
        for filename_stub in self.filename_stubs:
            print(filename_stub)
            self.do_test(filename_stub)

    def test_path_string_parsing(self):
        path = parse_position_string('1, 2, 3, 4')
        assert path == (1, ((1, None), (2, None), (3, None), (4, None)), ())

        path = parse_position_string('2,3[0:10:2],5[2].7[2].8')
        assert path == (
            1,
            ((2, None), (3, slice(0, 10, 2)), (5, 2)),
            ((7, 2), (8, None))
        )

        path = parse_position_string('#121, 1, 3, 5[3:].1. 3')
        assert path == (
            121,
            ((1, None), (3, None), (5, slice(3, None, None))),
            ((1, None), (3, None))
        )

    def test_query_by_path(self):
        s = read_bufr_file('asr3_190.bufr')
        bufr = self.decoder.decode(s, 'asr3_190')
        bufr_data = bufr.wire_data()

        assert bufr_data.query_by_position('1,1,3')[1] == 333
        assert bufr_data.query_by_position('#1,1,1,3')[1] == 333
        assert bufr_data.query_by_position('#128,1,1,3')[1] == 333

        assert bufr_data.query_by_position('#1, 1, 4, 1')[1] == 24.87108
        assert bufr_data.query_by_position('#3, 1, 4, 1')[1] == 24.87502

        assert bufr_data.query_by_position('#1, 1, 11, 1, 2')[1] == [
            236391100000000.0, 131971700000000.0, 64338200000000.0,
            36184200000000.0, 30148700000000.0, 11316000000000.0,
            6395700000000.0, 3612800000000.0, 10653400000000.0,
            8571400000000.0, 6835300000000.0
        ]

        assert bufr_data.query_by_position('#1, 1, 11, 1, 3.2.1')[1] == [
            10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10
        ]

        assert bufr_data.query_by_position('1, 11, 1, 3.1')[1] == \
               bufr_data.query_by_position('7, 1')[1][0::6]

        assert bufr_data.query_by_position('#128, 1, 11, 1, 14')[1] == [
            None, None, None, 240.7, 215.7, 220.3, 227.1, 228.3, 224.2, 221.5, 218.3
        ]

    def test_query_by_name(self):
        s = read_bufr_file('asr3_190.bufr')
        bufr = self.decoder.decode(s, 'asr3_190')
        bufr_data = bufr.wire_data()

        assert bufr_data.query_by_name('001007')[1] == [[57]] * 128
        assert bufr_data.query_by_name('012063')[1][-1] == [
            None, None, None, None, None, None,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
            240.7, None, 240.7, None, None, 240.7,
            215.7, None, 215.7, None, None, 215.7,
            220.3, None, 220.3, None, None, 220.3,
            227.1, None, 227.1, None, None, 227.1,
            228.3, None, 228.3, None, None, 228.3,
            224.2, None, 224.2, None, None, 224.2,
            221.5, None, 221.5, None, None, 221.5,
            218.3, None, 218.3, None, None, 218.3
        ]

        assert bufr_data.query_by_name('012063.F12063.008023')[1] == [[10] * 66] * 128
