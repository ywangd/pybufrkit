from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import hashlib
import logging
import unittest

from pybufrkit.decoder import Decoder, generate_bufr_message
from pybufrkit.renderer import FlatTextRenderer

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s: %(funcName)s: %(message)s")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


class BenchMarkTests(unittest.TestCase):
    def setUp(self):
        self.decoder = Decoder()

    def test(self):
        output = []
        with open(os.path.join(DATA_DIR, 'prepbufr.bufr')) as ins:
            for bufr_message in generate_bufr_message(self.decoder, ins.read()):
                output.append(FlatTextRenderer().render(bufr_message))

        assert 'a2708bc6464c9c541d2555fef64d9fb6' == hashlib.md5(
            '\n'.join(output)).hexdigest(), 'prepbufr decoding error'
