from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import sys

from pybufrkit.decoder import Decoder
from pybufrkit.renderer import FlatTextRenderer

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s: %(funcName)s: %(message)s")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')

compare = """  249 001031 IDENTIFICATION OF ORIGINATING/GENERATING CENTRE (SEE NOTE 10)       None
  250 001032 GENERATING APPLICATION                                              None
  251 033036 NOMINAL CONFIDENCE THRESHOLD                              ->     16 None
  252 033036 NOMINAL CONFIDENCE THRESHOLD                              ->     17 None
  253 033036 NOMINAL CONFIDENCE THRESHOLD                              ->     18 None
  254 033036 NOMINAL CONFIDENCE THRESHOLD                                        None
<<<<<< section 5 >>>>>>
stop_signature = b'7777'"""


def test_mismatched_bitmap():
    decoder = Decoder()
    with open(os.path.join(DATA_DIR, 'ncep.352.bufr'), 'rb') as ins:
        bufr_message = decoder.process(ins.read())
        output = FlatTextRenderer().render(bufr_message)

    assert output.endswith(compare)
