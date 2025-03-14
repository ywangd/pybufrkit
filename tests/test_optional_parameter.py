from pybufrkit.decoder import Decoder
from pybufrkit.encoder import Encoder

# Special thanks to https://github.com/ReallyNiceGuy for identifying the bug and providing the test case.
data = [
    [
        b'BUFR',
        0,  # file length (will be calculated)
        4
    ],
    [0,  # section length (will be calculated)
     0,  # master table
     0,  # centre
     0,  # subcentre
     0,  # sequence number
     False,  # has section 2 (no)
     '0000000',  # flag bits
     6,  # data category
     0,  # i18n subcategory
     2,  # data local subcategory
     11,  # master table version
     10,  # local table version
     0,  # year
     0,  # month
     0,  # day
     0,  # hour
     0,  # min
     0,  # sec
     b'\xff\xff'],  # Extra bytes
    [0,  # section length (will be calculated)
     '00000000',  # reserved bits
     1,  # subsets
     True,  # is observation
     False,  # is compressed
     '000000',  # flag bits
     # Definition follows
     []
     ],
    [
        0,  # section length (will be calculated)
        '00000000',  # flag bits
        [
            [
            ]  # flat data
        ]
    ],
    [b'7777']
]


def test_overriding_master_table_version():
    encoder = Encoder()
    bufr_message = encoder.process(data)
    assert bufr_message.sections[1].section_length.value == 24
    assert bufr_message.local_bytes.value == b'\xff\xff'

    decoder = Decoder()
    decoded = decoder.process(bufr_message.serialized_bytes)
    assert decoded.sections[1].section_length.value == 24
    assert decoded.local_bytes.value == b'\xff\xff'
