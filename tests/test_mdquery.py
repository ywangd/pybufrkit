import os

import pytest

from pybufrkit.errors import MetadataExprParsingError
from pybufrkit.decoder import Decoder
from pybufrkit.mdquery import MetadataExprParser, MetadataQuerent

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


metadata_expr_parser = MetadataExprParser()
md_querent = MetadataQuerent(metadata_expr_parser)
decoder = Decoder()


def test_simple_query():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '%length') == 5004
    assert md_querent.query(bufr_message, '%edition') == 3


def test_default_is_first_match():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '%section_length') == 18


def test_explicit_section_index():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '%3.section_length') == 154
    assert md_querent.query(bufr_message, '%4.section_length') == 4768


def test_unexpanded_descriptors():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '%unexpanded_descriptors') == [
        1007, 25060, 1033, 2048, 2048, 5040, 201134,
        7001, 201000, 202131, 7005, 202000, 301011,
        301012, 4007, 5001, 6001, 8029, 8074, 8012,
        25095, 25096, 25097, 204001, 31021, 22070,
        204000, 8023, 22070, 21128, 123002, 8076,
        204001, 31021, 201129, 21062, 201000, 204000,
        8023, 21062, 204001, 31021, 201134, 7001,
        201000, 204000, 202131, 7005, 202000, 8023,
        202131, 7001, 202000, 21128, 204001, 31021,
        2173, 204000, 107003, 201130, 2121, 201000,
        204001, 31021, 12163, 204000, 104002, 2023,
        202129, 11012, 202000, 13090, 13091
    ]


def test_stripping_whites():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '   %length  ') == 5004


def test_non_exist_metadata():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '%blahblah') is None


def test_non_exist_section():
    s = read_bufr_file('jaso_214.bufr')
    bufr_message = decoder.process(s)
    assert md_querent.query(bufr_message, '%9.length') is None


def test_error_no_leading_dollar_sign():
    with pytest.raises(MetadataExprParsingError):
        metadata_expr_parser.parse('length')


def test_error_invalid_section_index():
    with pytest.raises(MetadataExprParsingError):
        metadata_expr_parser.parse('%a.length')
