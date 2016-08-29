from __future__ import absolute_import
import os
import unittest

from pybufrkit.tables import get_table_group
from pybufrkit.descriptorsplus import VirtualBufrTemplate, VirtualTemplateParser

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


class VirtualTemplateTests(unittest.TestCase):
    def setUp(self):
        self.table_group = get_table_group()

    def test_build_virtual_template(self):
        template = self.table_group.template_from_ids(
            309052, 1081, 1082, 2067, 2095, 2096, 2097, 2017, 2191, 25061, 205060)

        vt = VirtualBufrTemplate.from_actual_template(template, name='Virtual')

        with open(os.path.join(DATA_DIR, 'build_virtual_template.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

    def test_remove_virtual_sequence(self):
        template = self.table_group.template_from_ids(
            309052, 1081, 1082, 2067, 2095, 2096, 2097, 2017, 2191, 25061, 205060)

        vt = VirtualBufrTemplate.from_actual_template(template, name='Virtual')

        vt.remove_virtual_descriptor(1)

        with open(os.path.join(DATA_DIR, 'remove_virtual_sequence.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

    def test_remove_virtual_fixed_replication(self):
        template = self.table_group.template_from_ids(307080)
        # print template.dumps()
        vt = VirtualBufrTemplate.from_actual_template(template, name='Virtual')

        vt.remove_virtual_descriptor(1, 5, 1)

        with open(os.path.join(DATA_DIR, 'remove_virtual_fixed_replication.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

    def test_insert_virtual_sequence(self):
        template = self.table_group.template_from_ids(
            309052, 1081, 1082, 2067, 2095, 2096, 2097, 2017, 2191, 25061, 205060)
        vt = VirtualBufrTemplate.from_actual_template(template, name='Virtual')

        vt.insert_virtual_sequence_descriptor(10, 2)  # insert a virtual sequence of 10 items at position 2
        vt.insert_virtual_sequence_descriptor(3, 2, 1)  # insert again a virtual sequence of 3 items at position 2, 1

        with open(os.path.join(DATA_DIR, 'insert_virtual_sequence.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

    def test_insert_virtual_fixed_replication(self):
        template = self.table_group.template_from_ids(
            309052, 1081, 1082, 2067, 2095, 2096, 2097, 2017, 2191, 25061, 205060)
        vt = VirtualBufrTemplate.from_actual_template(template, name='Virtual')

        # Insert a virtual fixed replication of 1 items and 3 repeats at position 1, 4, 4
        vt.insert_virtual_fixed_replication_descriptor(1, 3, 1, 4, 4)

        with open(os.path.join(DATA_DIR, 'insert_virtual_fixed_replication.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

    def test_remove_all_virtual_sequences(self):
        template = self.table_group.template_from_ids(309052)
        vt = VirtualBufrTemplate.from_actual_template(template, name='LEMM0')
        vt.remove_all_virtual_sequence_descriptors()

        with open(os.path.join(DATA_DIR, 'remove_all_virtual_sequences.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

    def test_virtual_template_parser(self):
        parser = VirtualTemplateParser(self.table_group)

        with open(os.path.join(DATA_DIR, 'IUSK73_AMMC.vtmpl')) as ins:
            s = ins.read()
        vt = parser.parse(s)

        with open(os.path.join(DATA_DIR, 'remove_all_virtual_sequences.cmp')) as ins:
            cmp_str = ins.read()
        assert vt.dumps() == cmp_str

        # TODO: test attributed element descriptor parsing?

    def test_serialize(self):
        parser = VirtualTemplateParser(self.table_group)

        with open(os.path.join(DATA_DIR, 'IUSK73_AMMC.vtmpl')) as ins:
            s = ins.read()
        vt = parser.parse(s)

        restored_vt = parser.parse(vt.serialize())

        with open(os.path.join(DATA_DIR, 'remove_all_virtual_sequences.cmp')) as ins:
            cmp_str = ins.read()
        assert restored_vt.dumps() == cmp_str
