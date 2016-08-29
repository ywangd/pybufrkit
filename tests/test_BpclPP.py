from __future__ import absolute_import
import os
import unittest

from pybufrkit.bpclpp import bpcl_pp

BASE_DIR = os.path.dirname(__file__)

class PreProcessorTests(unittest.TestCase):
    def test_basic_include(self):
        file_path = os.path.join(
            BASE_DIR, 'data', 'preprocessor_tests_02.bpcl'
        )

        s = bpcl_pp.process(file_path)

        assert s == 'x = 3\nprint "hello world"\ny = 42'

    def test_nested_include(self):
        file_path = os.path.join(
            BASE_DIR, 'data', 'preprocessor_tests_01.bpcl'
        )

        s = bpcl_pp.process(file_path)

        assert s == 'a = 0\nx = 3\nprint "hello world"\ny = 42\nc = 3'

    def test_line_continuation(self):
        file_path = os.path.join(BASE_DIR, 'data', 'bpclpp_test_line_continuation.bpcl')
        s = bpcl_pp.process(file_path)
        assert s == "x = 42\na = 1 +      2 +      3\nf()"

