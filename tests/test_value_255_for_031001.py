from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import sys
import unittest

from pybufrkit.decoder import Decoder
from pybufrkit.encoder import Encoder
from pybufrkit.renderer import FlatTextRenderer
from pybufrkit.utils import flat_text_to_flat_json

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s: %(funcName)s: %(message)s")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')

compare = """ 2536 006002 LONGITUDE (COARSE ACCURACY)                                         162.24
 2537 005002 LATITUDE (COARSE ACCURACY)                                          -37.01
 2538 006002 LONGITUDE (COARSE ACCURACY)                                         161.45
 2539 005002 LATITUDE (COARSE ACCURACY)                                          -37.22
 2540 006002 LONGITUDE (COARSE ACCURACY)                                         160.57
 2541 020008 CLOUD DISTRIBUTION FOR AVIATION                                     10
 2542 020012 CLOUD TYPE                                                          9
 2543 008007 DIMENSIONAL SIGNIFICANCE                                            None
 2544 008011 METEOROLOGICAL FEATURE                                              None
<<<<<< section 5 >>>>>>
stop_signature = b'7777'"""

class TestValue255For031001(unittest.TestCase):

    def test_uncompressed(self):
        output = self.decode()
        json = flat_text_to_flat_json(output)

        with self.assertRaises(FileNotFoundError):
            encoder = Encoder()
            encoder.process(json)

        encoder = Encoder(fallback_or_ignore_missing_tables=True)
        bufr_message = encoder.process(json)

        assert FlatTextRenderer().render(bufr_message) == output

    def test_compressed(self):
        output = self.decode().replace('is_compressed = False', 'is_compressed = True')
        json = flat_text_to_flat_json(output)

        encoder = Encoder(fallback_or_ignore_missing_tables=True)
        bufr_message = encoder.process(json)
        # output from compressed message has a different length due to compression
        compressed_output = FlatTextRenderer().render(bufr_message)
        self.assert_output(compressed_output)

        decoder = Decoder()
        decoded = decoder.process(bufr_message.serialized_bytes)
        assert FlatTextRenderer().render(decoded) == compressed_output


    def decode(self):
        decoder = Decoder()
        with open(os.path.join(DATA_DIR, 'JUBE99_EGRR.bufr'), 'rb') as ins:
            bufr_message = decoder.process(ins.read())
            output = FlatTextRenderer().render(bufr_message)
        self.assert_output(output)
        return output

    def assert_output(self, output):
        assert '1901 031001 DELAYED DESCRIPTOR REPLICATION FACTOR                               255' in output
        assert output.endswith(compare)