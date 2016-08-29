from __future__ import absolute_import
import unittest
from collections import OrderedDict

from pybufrkit.bpclcompiler import bpcl_compiler


class CompilerTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test(self):
        g = OrderedDict()
        exec(bpcl_compiler.compile(
            'x = 42\na = 5 + 3 * 2\nif a > 10 { y = 1 } else { y = -1 }'
        ), g)
        keys = [k for k in g.keys() if not k.startswith('__')]
        assert keys == ['x', 'a', 'y']
        assert g['x'] == 42
        assert g['a'] == 11
        assert g['y'] == 1
