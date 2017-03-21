import os

from pybufrkit.decoder import Decoder
from pybufrkit.renderer import FlatJsonRenderer, NestedJsonRenderer
from pybufrkit.utils import nested_json_to_flat_json

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


decoder = Decoder()
nested_json_renderer = NestedJsonRenderer()
flat_json_renderer = FlatJsonRenderer()


def test_nested_json_to_flat_json_1():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    nested = nested_json_renderer.render(bufr_message)
    flat = flat_json_renderer.render(bufr_message)

    assert flat == nested_json_to_flat_json(nested)


def test_nested_json_to_flat_json_2():
    s = read_bufr_file('asr3_190.bufr')
    bufr_message = decoder.process(s)
    nested = nested_json_renderer.render(bufr_message)
    flat = flat_json_renderer.render(bufr_message)

    assert flat == nested_json_to_flat_json(nested)
