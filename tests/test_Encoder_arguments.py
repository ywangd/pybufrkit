from pybufrkit.encoder import Encoder
from pybufrkit.decoder import Decoder

CONTRIVED_JSON = """[["BUFR", 94, 4], [22, 0, 1, 0, 0, false, "0000000", 2, 4, 0, 18, 0, 2016, 2, 18, 23, 0, 0], [25, "00000000", 2, true, false, "000000", [301001, 105002, 102000, 31001, 8002, 20011, 8002, 301011, 20011]], [35, "00000000", [[94, 461, 2, 1, 2, 3, 4, 21, 3, 5, 6, 7, 8, 9, 10, 22, 2016, 2, 18, 1], [95, 888, 3, 12, 11, 10, 9, 8, 7, 22, 2, 6, 5, 4, 3, 21, 2017, 1, 1, 2]]], ["7777"]]"""


def test_overriding_master_table_version():
    encoder = Encoder(master_table_version=31)
    bufr_message = encoder.process(CONTRIVED_JSON)
    assert 31 == bufr_message.master_table_version.value
    assert 31 == Decoder().process(bufr_message.serialized_bytes).master_table_version.value
