from __future__ import absolute_import
from __future__ import print_function

import os
import unittest

from pybufrkit.dataquery import NodePathParser
from pybufrkit.errors import PathExprParsingError

BASE_DIR = os.path.dirname(__file__)


class NodePathParserTests(unittest.TestCase):
    def setUp(self):
        self.parser = NodePathParser()

    def test(self):
        slice_all = slice(None, None, None)

        p1 = self.parser.parse('/001001')
        assert p1.subset_slice == slice_all
        assert len(p1.components) == 1
        assert p1.components[0] == ('/', '001001', slice_all)

        p2 = self.parser.parse('@[0] / 103008 / 001001[:].008321[::]')
        assert p2.subset_slice == 0
        assert len(p2.components) == 3
        assert p2.components[0] == ('/', '103008', slice_all)
        assert p2.components[1] == ('/', '001001', slice(None, None))
        assert p2.components[2] == ('.', '008321', slice_all)

        p3 = self.parser.parse('@[0:10] /001008[1]. A01008')
        assert p3.subset_slice == slice(0, 10, None)
        assert p3.components[0] == ('/', '001008', 1)
        assert p3.components[1] == ('.', 'A01008', slice_all)

        p4 = self.parser.parse('@[-1] / 001008[::-1]')
        assert p4.subset_slice == slice(-1, None, None)
        assert p4.components[0] == ('/', '001008', slice(None, None, -1))

        p5 = self.parser.parse('@[0::10]/301011/004001')
        assert p5.subset_slice == slice(0, None, 10)
        assert p5.components[0] == ('/', '301011', slice_all)
        assert p5.components[1] == ('/', '004001', slice_all)

        p6 = self.parser.parse('@[-2] / 001011[-10]')
        assert p6.subset_slice == slice(-2, -1, None)
        assert p6.components[0] == ('/', '001011', slice(-10, -9, None))

        p7 = self.parser.parse('001001')
        assert p7.subset_slice == slice_all
        assert p7.components[0] == ('>', '001001', slice_all)

        p8 = self.parser.parse('>001001')
        assert p8.subset_slice == slice_all
        assert p8.components[0] == ('>', '001001', slice_all)

        p9 = self.parser.parse('/ 103002 > 010009[2] . A03101')
        assert p9.subset_slice == slice_all
        assert p9.components[0] == ('/', '103002', slice_all)
        assert p9.components[1] == ('>', '010009', 2)
        assert p9.components[2] == ('.', 'A03101', slice_all)

        p10 = self.parser.parse('@[0] > 020012')
        assert p10.subset_slice == 0
        assert p10.components[0] == ('>', '020012', slice_all)

        p11 = self.parser.parse('@[0] > 302035 / 302004 > 020012')
        assert p11.subset_slice == 0
        assert p11.components[0] == ('>', '302035', slice_all)
        assert p11.components[1] == ('/', '302004', slice_all)
        assert p11.components[2] == ('>', '020012', slice_all)

    def test_errors(self):
        with self.assertRaises(PathExprParsingError):
            self.parser.parse('')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('/')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('@/')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('/[0]')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('/001001[]')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('/001001[a]')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('/001001[:::]')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('@[:].001001')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('.001001')

        with self.assertRaises(PathExprParsingError):
            self.parser.parse('section_length')
