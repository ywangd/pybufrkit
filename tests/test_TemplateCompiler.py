from __future__ import absolute_import
from __future__ import print_function
import os
import json
import unittest

from pybufrkit.tables import TableGroupCacheManager
from pybufrkit.templatecompiler import TemplateCompiler, loads_compiled_template
from pybufrkit.decoder import Decoder

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


class TemplateCompilerTests(unittest.TestCase):
    def setUp(self):
        self.template_compiler = TemplateCompiler()

    def test_compile_serialize_and_deserialize(self):
        table_group = TableGroupCacheManager.get_table_group()
        descriptor_ids = [
            x.strip() for x in
            '311001, 222000, 101018, 31031, 1031, 1032, 101018, 33007'.split(',')
        ]
        template = table_group.template_from_ids(*descriptor_ids)
        compiled_template = self.template_compiler.process(template, table_group)

        reconstructed_compiled_template = loads_compiled_template(
            json.dumps(compiled_template.to_dict()))

        assert reconstructed_compiled_template.to_dict() == compiled_template.to_dict()

    def test_compiled_vs_noncompiled(self):
        decoder_noncompiled = Decoder()
        decoder_compiled = Decoder(compiled_template_cache_max=200)
        benchmark_data_dir = os.path.join(BASE_DIR, 'benchmark_data')

        for filename in os.listdir(benchmark_data_dir):
            with open(os.path.join(benchmark_data_dir, filename), 'rb') as ins:
                s = ins.read()

                bufr_message_1 = decoder_noncompiled.process(s)
                bufr_message_2 = decoder_compiled.process(s)
                assert bufr_message_1.template_data.value.decoded_descriptors_all_subsets == \
                       bufr_message_2.template_data.value.decoded_descriptors_all_subsets

                assert bufr_message_1.template_data.value.decoded_values_all_subsets == \
                       bufr_message_2.template_data.value.decoded_values_all_subsets

                assert bufr_message_1.template_data.value.bitmap_links_all_subsets == \
                       bufr_message_2.template_data.value.bitmap_links_all_subsets
