from __future__ import absolute_import
from __future__ import print_function

import os
import unittest

# noinspection PyUnresolvedReferences
from six.moves import range

from pybufrkit.decoder import Decoder
from pybufrkit.dataquery import NodePathParser, DataQuerent

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def read_bufr_file(file_name):
    with open(os.path.join(DATA_DIR, file_name), 'rb') as ins:
        s = ins.read()
    return s


class DataQueryTests(unittest.TestCase):
    def setUp(self):
        self.decoder = Decoder()
        self.querent = DataQuerent(NodePathParser())

    def test_query_jaso_214(self):
        s = read_bufr_file('jaso_214.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '/301011/004001')
        assert r1.subset_indices() == list(range(128))
        assert r1.all_values(flat=True) == [[2012] for _ in range(128)]
        assert r1.all_values() == [[2012] for _ in range(128)]

        r2 = self.querent.query(bufr_message, '@[0]/301011/004001')
        assert r2.subset_indices() == [0]
        assert r2.all_values(flat=True) == [[2012]]
        assert r2.all_values() == [[2012]]

        r3 = self.querent.query(bufr_message, '@[::10]/301011/004001')
        assert r3.subset_indices() == [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
        assert r3.all_values(flat=True) == [[2012] for _ in range(13)]

        r4 = self.querent.query(bufr_message, '@[1]/123002/021062')
        assert r4.subset_indices() == [1]
        assert r4.all_values(flat=True) == [[11.28, 0.02, 14.78, 0.03]]
        assert r4.all_values() == [[[[11.28, 0.02], [14.78, 0.03]]]]

        r5 = self.querent.query(bufr_message, '@[2:7:2]/123002/021062[0]')
        assert r5.subset_indices() == [2, 4, 6]
        assert r5.all_values(flat=True) == [[11.32, 14.77], [11.54, 14.95], [11.65, 15.24]]
        assert r5.all_values() == [
            [  # 1st subset
                [  # replication envelope
                    [11.32],  # replication block 1
                    [14.77]  # replication block 2
                ]
            ],
            [  # 2nd subset
                [[11.54], [14.95]]
            ],
            [  # 3rd subset
                [[11.65], [15.24]]
            ]
        ]

        r6 = self.querent.query(bufr_message, '@[-1]/123002/021062')
        assert r6.subset_indices() == [127]
        assert r6.all_values(flat=True) == [[12.8, 0.06, 16.4, 0.05]]
        assert r6.all_values() == [[[[12.8, 0.06], [16.4, 0.05]]]]

        r7 = self.querent.query(bufr_message, '/123002/021062[0].A21062.031021')
        assert r7.subset_indices() == list(range(128))
        assert r7.all_values(flat=True) == [[1, 1] for _ in range(128)]
        assert r7.all_values() == [[[[1], [1]]] for _ in range(128)]

        r8 = self.querent.query(bufr_message, '/002173.A02173')
        assert r8.subset_indices() == list(range(128))
        assert r8.all_values(flat=True) == [[0] for _ in range(128)]
        assert r8.all_values() == [[0] for _ in range(128)]

    def test_query_207003(self):
        s = read_bufr_file('207003.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '/310060/301021/006001')
        assert r1.subset_indices() == [0, 1]
        assert r1.all_values() == [[24.54144], [24.3926]]

        r2 = self.querent.query(bufr_message, '/310060/104000.031002')
        assert r2.subset_indices() == [0, 1]
        assert r2.all_values() == [[5], [5]]

        r3 = self.querent.query(bufr_message, '@[-1]/310060/104000/005042')
        assert r3.subset_indices() == [1]
        assert r3.all_values(flat=True) == [[1, 2, 3, 4, 5]]
        assert r3.all_values() == [[[[1], [2], [3], [4], [5]]]]

    def test_query_ISMD01_OKPR(self):
        s = read_bufr_file('ISMD01_OKPR.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '/307080/301090/301004/001015')
        assert r1.subset_indices() == list(range(7))
        values = [
            [b'Primda              '],
            [b'Kocelovice          '],
            [b'Praha-Ruzyne        '],
            [b'Liberec             '],
            [b'Pribyslav           '],
            [b'Brno-Turany         '],
            [b'Ostrava-Mosnov      '],
        ]
        assert r1.all_values(flat=True) == values
        assert r1.all_values() == values

    def test_query_amv2_87(self):
        s = read_bufr_file('amv2_87.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '/310195/303250/011001.033007')
        values = [
            [48, 35, 0],
            [54, 47, 0],
            [59, 68, 0],
            [56, 49, 0],
            [32, 37, 0],
            [48, 46, 0],
            [25, 31, 0],
            [49, 50, 0],
            [78, 75, 0],
            [84, 83, 0],
            [27, 34, 0],
            [60, 73, 0],
            [25, 31, 0],
            [26, 32, 0],
            [54, 68, 0],
            [32, 40, 0],
            [47, 49, 0],
            [31, 31, 0],
            [96, 95, 0],
            [91, 89, 0],
            [92, 98, 0],
            [82, 80, 0],
            [55, 62, 0],
            [50, 38, 0],
            [52, 54, 0],
            [89, 89, 0],
            [88, 86, 0],
            [94, 97, 0],
            [46, 42, 0],
            [65, 71, 0],
            [58, 49, 0],
            [67, 70, 0],
            [69, 73, 0],
            [60, 54, 0],
            [30, 37, 0],
            [36, 40, 0],
            [36, 40, 0],
            [30, 32, 0],
            [74, 91, 0],
            [73, 69, 0],
            [54, 56, 0],
            [93, 95, 0],
            [80, 87, 0],
            [88, 92, 0],
            [85, 83, 0],
            [50, 57, 0],
            [94, 97, 0],
            [25, 31, 0],
            [32, 40, 0],
            [49, 61, 0],
            [29, 37, 0],
            [26, 33, 0],
            [42, 53, 0],
            [34, 43, 0],
            [38, 47, 0],
            [33, 40, 0],
            [71, 79, 0],
            [43, 50, 0],
            [46, 57, 0],
            [49, 58, 0],
            [64, 79, 0],
            [70, 84, 0],
            [76, 94, 0],
            [74, 91, 0],
            [94, 98, 0],
            [67, 72, 0],
            [64, 76, 0],
            [82, 80, 0],
            [97, 98, 0],
            [82, 79, 0],
            [57, 48, 0],
            [68, 65, 0],
            [75, 69, 0],
            [67, 66, 0],
            [85, 91, 0],
            [68, 72, 0],
            [82, 86, 0],
            [38, 46, 0],
            [72, 79, 0],
            [43, 49, 0],
            [32, 34, 0],
            [39, 45, 0],
            [37, 43, 0],
            [78, 89, 0],
            [91, 98, 0],
            [92, 98, 0],
            [95, 96, 0],
            [90, 88, 0],
            [69, 69, 0],
            [64, 66, 0],
            [40, 49, 0],
            [54, 66, 0],
            [31, 35, 0],
            [76, 90, 0],
            [70, 82, 0],
            [60, 72, 0],
            [58, 71, 0],
            [41, 51, 0],
            [58, 59, 0],
            [57, 56, 0],
            [74, 82, 0],
            [75, 93, 0],
            [76, 93, 0],
            [82, 96, 0],
            [90, 97, 0],
            [96, 98, 0],
            [90, 98, 0],
            [89, 97, 0],
            [90, 97, 0],
            [89, 94, 0],
            [97, 98, 0],
            [80, 75, 0],
            [92, 92, 0],
            [83, 84, 0],
            [66, 66, 0],
            [34, 36, 0],
            [83, 88, 0],
            [87, 88, 0],
            [67, 69, 0],
            [85, 89, 0],
            [36, 44, 0],
            [40, 48, 0],
            [24, 30, 0],
            [58, 66, 0],
            [71, 65, 0],
            [91, 98, 0],
            [91, 97, 0],
            [97, 98, 0],
        ]
        assert r1.all_values(flat=True) == values
        assert r1.all_values() == values

        r2 = self.querent.query(bufr_message, '/310195/303250/011001.033007[1]')
        values = [
            [35], [47], [68], [49], [37], [46], [31], [50], [75], [83], [34], [73], [31], [32], [68], [40], [49], [31],
            [95], [89], [98], [80], [62], [38], [54], [89], [86], [97], [42], [71], [49], [70], [73], [54], [37], [40],
            [40], [32], [91], [69], [56], [95], [87], [92], [83], [57], [97], [31], [40], [61], [37], [33], [53], [43],
            [47], [40], [79], [50], [57], [58], [79], [84], [94], [91], [98], [72], [76], [80], [98], [79], [48], [65],
            [69], [66], [91], [72], [86], [46], [79], [49], [34], [45], [43], [89], [98], [98], [96], [88], [69], [66],
            [49], [66], [35], [90], [82], [72], [71], [51], [59], [56], [82], [93], [93], [96], [97], [98], [98], [97],
            [97], [94], [98], [75], [92], [84], [66], [36], [88], [88], [69], [89], [44], [48], [30], [66], [65], [98],
            [97], [98]
        ]
        assert r2.all_values(flat=True) == values
        assert r2.all_values() == values

    def test_query_asr3_190(self):
        s = read_bufr_file('asr3_190.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '@[-1]/310028/101011/304037/012063.F12063')
        assert r1.subset_indices() == [127]
        assert r1.all_values(flat=True) == [
            [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
             17.3, None, 17.3, None, None, 17.3, 4.4, None, 4.3, None, None, 4.3, 7.7, None, 7.6, None, None, 7.6, 13.2,
             None, 13.2, None, None, 13.2, 8.4, None, 8.4, None, None, 8.4, 11.9, None, 11.9, None, None, 11.9, 10.5,
             None, 10.4, None, None, 10.4, 6.7, None, 6.6, None, None, 6.6]
        ]
        assert r1.all_values() == [
            [
                [
                    [None, None, None, None, None, None],
                    [None, None, None, None, None, None],
                    [None, None, None, None, None, None],
                    [17.3, None, 17.3, None, None, 17.3],
                    [4.4, None, 4.3, None, None, 4.3],
                    [7.7, None, 7.6, None, None, 7.6],
                    [13.2, None, 13.2, None, None, 13.2],
                    [8.4, None, 8.4, None, None, 8.4],
                    [11.9, None, 11.9, None, None, 11.9],
                    [10.5, None, 10.4, None, None, 10.4],
                    [6.7, None, 6.6, None, None, 6.6],
                ]
            ]
        ]

        r2 = self.querent.query(bufr_message, '@[-2]/310028/101011/304037/012063.F12063.008023')
        assert r2.subset_indices() == [126]
        assert r2.all_values(flat=True) == [[10] * 66]
        assert r2.all_values() == [
            [
                [[10] * 6] * 11
            ]
        ]

    def test_query_mpco_217(self):
        s = read_bufr_file('mpco_217.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '@[-3:]/116000/106000/010004')
        assert r1.subset_indices() == [125, 126, 127]
        assert r1.all_values(flat=True) == [
            [10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101109.2],
            [10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101099.8],
            [10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101090.1],
        ]
        assert r1.all_values() == [
            [
                [[
                    [[10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                     [60000.0], [70000.0], [80000.0], [90000.0], [101109.2]]
                ]]
            ],
            [
                [[
                    [[10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                     [60000.0], [70000.0], [80000.0], [90000.0], [101099.8]]
                ]]
            ],
            [
                [[
                    [[10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                     [60000.0], [70000.0], [80000.0], [90000.0], [101090.1]]
                ]]
            ],
        ]

    def test_query_rado_250(self):
        s = read_bufr_file('rado_250.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '/310226/107000/103000/015037.F15037.008023')
        assert r1.all_values(flat=True) == [[13] * 247]
        assert r1.all_values() == [[
            [[[[13]]]] * 247
        ]]

        r2 = self.querent.query(bufr_message, '/310226/107000.031002')
        assert r2.all_values(flat=True) == [[247]]
        assert r2.all_values() == [[247]]

        r3 = self.querent.query(bufr_message, '/310226/107000/103000.031001')
        assert r3.all_values(flat=True) == [[1] * 247]
        assert r3.all_values() == [[
            [[1]] * 247
        ]]

    def test_descendant_ISMD01_OKPR(self):
        s = read_bufr_file('ISMD01_OKPR.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '020012')
        assert r1.all_values(flat=True) == [
            [62, 61, 60, 59, None, None],
            [36, 61, 60, 7, None, None],
            [36, 61, 60, 7, None, None],
            [36, 61, 60, 7, None, None],
            [36, 61, 60, 7, None, None],
            [36, 61, 60, 7, None, None],
            [30, 20, 11, 0, None, None],
        ]
        assert r1.all_values() == [
            [62, 61, 60, [[59]], [[None]], None],
            [36, 61, 60, [[7]], [[None]], None],
            [36, 61, 60, [[7]], [[None]], None],
            [36, 61, 60, [[7]], [[None]], None],
            [36, 61, 60, [[7]], [[None]], None],
            [36, 61, 60, [[7]], [[None]], None],
            [30, 20, 11, [[0]], [[None]], None],
        ]

        r2 = self.querent.query(bufr_message, '302035 > 020012')
        assert r2.all_values(flat=True) == [
            [62, 61, 60, 59],
            [36, 61, 60, 7],
            [36, 61, 60, 7],
            [36, 61, 60, 7],
            [36, 61, 60, 7],
            [36, 61, 60, 7],
            [30, 20, 11, 0],
        ]
        assert r2.all_values() == [
            [62, 61, 60, [[59]]],
            [36, 61, 60, [[7]]],
            [36, 61, 60, [[7]]],
            [36, 61, 60, [[7]]],
            [36, 61, 60, [[7]]],
            [36, 61, 60, [[7]]],
            [30, 20, 11, [[0]]],
        ]

        r3 = self.querent.query(bufr_message, '@[0] > 302035 > 020012')
        assert r3.all_values(flat=True) == [[62, 61, 60, 59]]
        assert r3.all_values() == [[62, 61, 60, [[59]]]]

        r4 = self.querent.query(bufr_message, '@[-3] > 302035/302004 > 020012')
        assert r4.all_values(flat=True) == [[36, 61, 60]]
        assert r4.all_values() == [[36, 61, 60]]

        r5 = self.querent.query(bufr_message, '008002')
        assert r5.all_values(flat=True) == [
            [5, 5, 11, 7, 8, 9, None],
            [7, 1, 12, 7, 8, 9, None],
            [7, 1, 12, 7, 8, 9, None],
            [7, 1, 12, 7, 8, 9, None],
            [7, 1, 12, 7, 8, 9, None],
            [7, 1, 12, 7, 8, 9, None],
            [0, 1, 12, 7, 8, 9, None],
        ]
        assert r5.all_values() == [
            [5, [[5]], [[11]], [[7], [8], [9]], None],
            [7, [[1]], [[12]], [[7], [8], [9]], None],
            [7, [[1]], [[12]], [[7], [8], [9]], None],
            [7, [[1]], [[12]], [[7], [8], [9]], None],
            [7, [[1]], [[12]], [[7], [8], [9]], None],
            [7, [[1]], [[12]], [[7], [8], [9]], None],
            [0, [[1]], [[12]], [[7], [8], [9]], None],
        ]

        r6 = self.querent.query(bufr_message, '@[4] > 302047 > 008002')
        assert r6.all_values(flat=True) == [[7, 8, 9]]
        assert r6.all_values() == [[
            [[7], [8], [9]]
        ]]

    def test_descendant_mpco_217(self):
        s = read_bufr_file('mpco_217.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '@[0] > 010004')
        assert r1.all_values(flat=True) == [
            [
                10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101025.2,
                10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101025.2
            ]
        ]
        assert r1.all_values() == [
            [
                [
                    [10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                    [60000.0], [70000.0], [80000.0], [90000.0], [101025.2]
                ],
                [[
                    [[10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                     [60000.0], [70000.0], [80000.0], [90000.0], [101025.2]]
                ]]
            ]
        ]

        r2 = self.querent.query(bufr_message, '@[0]/116000 > 010004')
        assert r2.all_values(flat=True) == [
            [10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101025.2]
        ]
        assert r2.all_values() == [
            [
                [[
                    [[10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                     [60000.0], [70000.0], [80000.0], [90000.0], [101025.2]]
                ]]
            ]
        ]

        r2 = self.querent.query(bufr_message, '@[0] > 010004[::10]')
        assert r2.all_values(flat=True) == [
            [
                10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101025.2,
                10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 101025.2
            ]
        ]
        assert r2.all_values() == [
            [
                [
                    [10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                    [60000.0], [70000.0], [80000.0], [90000.0], [101025.2]
                ],
                [[
                    [[10000.0], [20000.0], [30000.0], [40000.0], [50000.0],
                     [60000.0], [70000.0], [80000.0], [90000.0], [101025.2]]
                ]]
            ]
        ]

    def test_contrived(self):
        s = read_bufr_file('contrived.bufr')
        bufr_message = self.decoder.process(s)

        r1 = self.querent.query(bufr_message, '/105002/102000/020011')
        assert r1.all_values(flat=True) == [
            [2, 4, 6, 8, 10],
            [11, 9, 7, 5, 3]
        ]
        assert r1.all_values() == [
            [
                [[[[2], [4]]], [[[6], [8], [10]]]]
            ],
            [
                [[[[11], [9], [7]]], [[[5], [3]]]]
            ]
        ]

        r2 = self.querent.query(bufr_message, '020011')
        assert r2.all_values(flat=True) == [
            [2, 4, 6, 8, 10, 1],
            [11, 9, 7, 5, 3, 2]
        ]
        assert r2.all_values() == [
            [
                [[[[2], [4]]], [[[6], [8], [10]]]], 1
            ],
            [
                [[[[11], [9], [7]]], [[[5], [3]]]], 2
            ]
        ]

        r3 = self.querent.query(bufr_message, '008002')
        assert r3.all_values(flat=True) == [
            [1, 3, 21, 5, 7, 9, 22],
            [12, 10, 8, 22, 6, 4, 21]
        ]
        assert r3.all_values() == [
            [
                [[[[1], [3]], 21], [[[5], [7], [9]], 22]]
            ],
            [
                [[[[12], [10], [8]], 22], [[[6], [4]], 21]]
            ]
        ]

        r4 = self.querent.query(bufr_message, '102000/008002')
        assert r4.all_values(flat=True) == [
            [1, 3, 5, 7, 9],
            [12, 10, 8, 6, 4]
        ]
        assert r4.all_values() == [
            [
                [[[[1], [3]]], [[[5], [7], [9]]]]
            ],
            [
                [[[[12], [10], [8]]], [[[6], [4]]]]
            ]
        ]
