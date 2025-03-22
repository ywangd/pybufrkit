from pybufrkit.bufr import BufrMessage


def test_bufr_message():
    bufr_message = BufrMessage()
    assert bufr_message.master_table_number.value == 0
    assert bufr_message.originating_subcentre.value == 0
