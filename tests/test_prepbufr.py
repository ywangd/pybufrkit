from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import logging
import unittest

from pybufrkit.decoder import Decoder, generate_bufr_message
from pybufrkit.renderer import FlatTextRenderer

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s: %(funcName)s: %(message)s")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')

compare = """  405 012004 T2MS     TABLE B ENTRY - 2-METER TEMPERATURE                        294.6
  406 013198 Q2MS     TABLE B ENTRY - 2-METER SPECIFIC HUMIDITY                  0.0083
  407 013232 WXTS     TABLE B ENTRY - SNOW PRECIP TYPE                           0
  408 013233 WXTP     TABLE B ENTRY - ICE PELLET PRECIP TYPE                     0
  409 013234 WXTZ     TABLE B ENTRY - FREEZING RAIN PRECIP TYPE                  0
  410 013235 WXTR     TABLE B ENTRY - RAIN PRECIP TYPE                           0
  411 031001 DRF8BIT                                                             3
  412 S63255 1 bits                                                              0
  413 S63255 1 bits                                                              0
  414 S63255 1 bits                                                              0
<<<<<< section 5 >>>>>>
"""


class BenchMarkTests(unittest.TestCase):
    def setUp(self):
        self.decoder = Decoder()

    def test(self):
        output = []
        with open(os.path.join(DATA_DIR, 'prepbufr.bufr'), 'rb') as ins:
            for bufr_message in generate_bufr_message(self.decoder, ins.read()):
                output.append(FlatTextRenderer().render(bufr_message))

        lines = [line for line in ('\n'.join(output)).splitlines(True)
                 if not line.startswith('TableGroupKey') and not line.startswith('stop_signature')]
        assert ''.join(lines).endswith(compare)
