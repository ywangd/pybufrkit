from __future__ import absolute_import
from __future__ import print_function
import os
import unittest

from pybufrkit.decoder import Decoder
from pybufrkit.renderer import NestedTextRenderer

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


class TemplateDataTests(unittest.TestCase):
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
        bufr_message = self.decoder.process(s, filename_stub)

        if filename_stub in ('207003', 'rado_250'):
            with open(os.path.join(DATA_DIR,
                                   '{}.datadump.cmp'.format(filename_stub))) as ins:
                cmp_str = ins.read()
            dump_str = NestedTextRenderer().render(bufr_message.template_data.value)
            # TODO: this is to fix the inconsistent int and long of bitstring on different OS
            dump_str = dump_str.replace(
                '005040 ORBIT NUMBER 5258\n', '005040 ORBIT NUMBER 5258L\n')
            assert dump_str == cmp_str, dump_str
        else:
            NestedTextRenderer().render(bufr_message.template_data.value)

    def test_template_data(self):
        print()
        for filename_stub in self.filename_stubs:
            print(filename_stub)
            self.do_test(filename_stub)
