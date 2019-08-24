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

        lines = [line for line in ('\n'.join(output) + '\n').splitlines(True) if not line.startswith('TableGroupKey')]
        assert '721809d8e376946890636c24dfb479ac' == hashlib.md5(''.join(lines)).hexdigest(), 'prepbufr decoding error'
