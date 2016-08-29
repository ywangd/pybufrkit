from __future__ import absolute_import
import os
import ast
import unittest

from pybufrkit.bpclcompiler import bpcl_compiler
from six import PY3
from six.moves import zip

BASE_DIR = os.path.dirname(__file__)


class ParserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if PY3:
            with open(os.path.join(BASE_DIR,
                                   'data', 'parser_tests_01.py3.bpcl')) as ins:
                lines = ins.readlines()
        else:
            with open(os.path.join(BASE_DIR,
                                   'data', 'parser_tests_01.bpcl')) as ins:
                lines = ins.readlines()

        cls.input_list = []
        cls.cmp_list = []
        input = []
        for line in lines:
            if line.startswith('## '):
                cls.input_list.append(''.join(input))
                input = []
                cls.cmp_list.append(line[3:].strip())
            else:
                input.append(line)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_01(self):
        for input_string, cmp_string in zip(self.input_list, self.cmp_list):
            tree = bpcl_compiler.parse(input_string)
            tree_string = ast.dump(tree)
            assert tree_string == cmp_string

    def test_02(self):
        with open(os.path.join(os.path.dirname(__file__),
                               'data', 'parser_tests_02.bpcl')) as ins:
            s = ins.read()

        tree = bpcl_compiler.parse(s)
        if PY3:
            with open(os.path.join(BASE_DIR, 'data', 'parser_tests_02.py3.cmp')) as ins:
                cmp_str = ins.read().strip()
        else:
            with open(os.path.join(BASE_DIR, 'data', 'parser_tests_02.cmp')) as ins:
                cmp_str = ins.read().strip()

        assert ast.dump(tree) == cmp_str
