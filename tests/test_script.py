from __future__ import print_function
from __future__ import absolute_import

import os

from pybufrkit.decoder import Decoder
from pybufrkit.script import process_embedded_query_expr, ScriptRunner

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')

decoder = Decoder()


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


def test_simple():
    script = 'length = ${%length}'
    s, variables = process_embedded_query_expr(script)
    assert s == 'length = PBK_0'

# def test_no_processing_comments():
#     script = '# length = ${%length}'
#     s, variables = process_embedded_query_expr(script)
#     assert s == '# length = ${%length}'

def test_single_var_for_multi_instances_of_same_query_expr():
    script = 'length = ${%length}\nanother_length = ${%length}'
    s, variables = process_embedded_query_expr(script)
    assert s == 'length = PBK_0\nanother_length = PBK_0'


def test():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    script = 'print("The length is", ${%length})'
    ScriptRunner(script).run(bufr_message)
