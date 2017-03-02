from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import logging
import unittest

from pybufrkit.decoder import Decoder

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s: %(funcName)s: %(message)s")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'benchmark_data')


class BenchMarkTests(unittest.TestCase):
    def setUp(self):
        self.decoder = Decoder()

    def test(self):
        for filename in os.listdir(DATA_DIR):
            with open(os.path.join(DATA_DIR, filename), 'rb') as ins:
                print(filename)
                bufr_message = self.decoder.process(ins.read(), wire_template_data=True)
                self.assertIsNotNone(bufr_message)
