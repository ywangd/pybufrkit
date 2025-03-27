from pybufrkit.decoder import Decoder
from pybufrkit.encoder import Encoder

data = [
    [
        b'BUFR',
        0,  # file length (will be calculated)
        2
    ],
    [0,  # section length (will be calculated)
     0,  # master table
     0,  # centre
     0,  # sequence number
     False,  # has section 2 (no)
     '0000000',  # flag bits
     6,  # data category
     0,  # data local subcategory
     11,  # master table version
     10,  # local table version
     25,  # year
     3,  # month
     25,  # day
     13,  # hour
     45,  # min
     b'localdata',  # Extra bytes
    ],
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


def test_optional_parameter_v2():
    encoder = Encoder()
    bufr_message = encoder.process(data)
    print(bufr_message.serialized_bytes)
    assert bufr_message.sections[1].section_length.value == 26
    assert bufr_message.local_bytes.value == b'localdata'

    decoder = Decoder()
    decoded = decoder.process(bufr_message.serialized_bytes)
    assert decoded.sections[1].section_length.value == 26
    assert decoded.local_bytes.value == b'localdata'
