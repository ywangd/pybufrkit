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


def test_pragma():
    # Default value
    code = 'print("something")'
    runner = ScriptRunner(code)
    assert runner.pragma['data_values_nest_level'] == 1

    # Override from inside script
    code = '#$ data_values_nest_level = 4\nprint("something")'
    runner = ScriptRunner(code)
    assert runner.pragma['data_values_nest_level'] == 4

    # Override from method keyword
    runner = ScriptRunner(code, data_values_nest_level=0)
    assert runner.pragma['data_values_nest_level'] == 0


def test_embedded_query():
    script = 'length = ${%length}; v = ${001001}'
    s, variables = process_embedded_query_expr(script)
    assert s == 'length = PBK_0; v = PBK_1'


def test_no_processing_comments():
    script = 'length = ${%length}  # length = ${%length}\nsomething = ${001001}'
    s, variables = process_embedded_query_expr(script)
    assert s == 'length = PBK_0  # length = ${%length}\nsomething = PBK_1'


def test_single_var_for_multi_instances_of_same_query_expr():
    script = 'length = ${%length}\nanother_length = ${%length}'
    s, variables = process_embedded_query_expr(script)
    assert s == 'length = PBK_0\nanother_length = PBK_0'


def test_assignment():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    script = 'print("The length is", ${%length})\na = ${%originating_centre}'
    variables = ScriptRunner(script).run(bufr_message)
    assert variables['a'] == 98


def test_variable_injection():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s, file_path='FILE.bufr')
    script = 'a = PBK_FILENAME'
    variables = ScriptRunner(script).run(bufr_message)
    assert variables['a'] == 'FILE.bufr'
