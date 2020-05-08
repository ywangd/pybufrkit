from __future__ import absolute_import
from __future__ import print_function

import os
import unittest

from pybufrkit.decoder import Decoder, generate_bufr_message

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


class MessageGeneratorTests(unittest.TestCase):

    def setUp(self):
        self.decoder = Decoder()

    def test_can_continue_on_error(self):
        bufr_messages = [m for m in generate_bufr_message(
            self.decoder,
            read_bufr_file('multi_invalid_messages.bufr'),
            continue_on_error=True)]
        assert len(bufr_messages) == 1, 'expect one good message'

    def test_can_filter_message(self):
        bufr_messages = [m for m in generate_bufr_message(
            self.decoder,
            read_bufr_file('multi_invalid_messages.bufr'),
            filter_expr='${%data_category} == 2')]
        assert len(bufr_messages) == 1, 'expect one message to be filtered'
