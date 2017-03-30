import os

from pybufrkit.decoder import Decoder
from pybufrkit.renderer import FlatTextRenderer, NestedTextRenderer, FlatJsonRenderer, NestedJsonRenderer
from pybufrkit.utils import nested_json_to_flat_json, flat_text_to_flat_json, nested_text_to_flat_json

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')

FILES = (
    '207003.bufr',
    'amv2_87.bufr',
    'asr3_190.bufr',
    'b002_95.bufr',
    'b005_89.bufr',
    'contrived.bufr',
    'g2nd_208.bufr',
    'ISMD01_OKPR.bufr',
    'IUSK73_AMMC_040000.bufr',
    'IUSK73_AMMC_182300.bufr',
    'jaso_214.bufr',
    'mpco_217.bufr',
    'profiler_european.bufr',
    'rado_250.bufr',
    'uegabe.bufr',
)


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


decoder = Decoder()
flat_text_renderer = FlatTextRenderer()
nested_text_renderer = NestedTextRenderer()
flat_json_renderer = FlatJsonRenderer()
nested_json_renderer = NestedJsonRenderer()


def test_nested_json_to_flat_json():
    def func(filename):
        s = read_bufr_file(filename)
        bufr_message = decoder.process(s)
        nested = nested_json_renderer.render(bufr_message)
        flat = flat_json_renderer.render(bufr_message)
        assert flat == nested_json_to_flat_json(nested)

    for filename in FILES:
        func(filename)


def test_flat_text_to_flat_json():
    def func(filename):
        s = read_bufr_file(filename)
        bufr_message = decoder.process(s)
        flat_text = flat_text_renderer.render(bufr_message)
        flat_json = flat_text_to_flat_json(flat_text)
        assert flat_json == flat_json_renderer.render(bufr_message)

    for filename in FILES:
        func(filename)


def test_nested_text_to_flat_json():
    def func(filename):
        s = read_bufr_file(filename)
        bufr_message = decoder.process(s)
        nested_text = nested_text_renderer.render(bufr_message)
        flat_json = nested_text_to_flat_json(nested_text)
        assert flat_json == flat_json_renderer.render(bufr_message)

    for filename in FILES:
        func(filename)
