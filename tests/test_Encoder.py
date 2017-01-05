from __future__ import absolute_import
from __future__ import print_function
import os
import unittest

from pybufrkit.encoder import Encoder
from pybufrkit.decoder import Decoder
import six
# noinspection PyUnresolvedReferences
from six.moves import range

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


class EncoderTests(unittest.TestCase):
    def setUp(self):
        self.encoder = Encoder(ignore_declared_length=True)
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

    def do_test(self, filename_stub):
        with open(os.path.join(DATA_DIR, filename_stub + '.json')) as ins:
            s = ins.read()
        bufr_message_encoded = self.encoder.process(s)
        bufr_message_decoded = self.decoder.process(bufr_message_encoded.serialized_bytes)

        assert len(bufr_message_encoded.template_data.value.decoded_values_all_subsets) == \
               len(bufr_message_decoded.template_data.value.decoded_values_all_subsets)

        for idx_subset in range(len(bufr_message_encoded.template_data.value.decoded_values_all_subsets)):
            encoder_values = bufr_message_encoded.template_data.value.decoded_values_all_subsets[idx_subset]
            decoder_values = bufr_message_decoded.template_data.value.decoded_values_all_subsets[idx_subset]
            assert len(encoder_values) == len(decoder_values)
            for idx_value in range(len(encoder_values)):
                if isinstance(encoder_values[idx_value], six.text_type):
                    encoder_value = encoder_values[idx_value].encode('latin-1')
                else:
                    encoder_value = encoder_values[idx_value]
                assert encoder_value == decoder_values[idx_value], \
                    '{!r} != {!r}'.format(encoder_value, decoder_values[idx_value])

    def test_encode(self):
        print()
        for filename_stub in self.filename_stubs:
            print(filename_stub)
            self.do_test(filename_stub)
