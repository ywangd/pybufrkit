"""
Utilities are not to be accessed directly by users in BPCL files.
"""
from __future__ import absolute_import
import itertools
from collections import OrderedDict

import bitstring
from six.moves import range

INDENT_CHARS = '    '


class BpclError(Exception):
    pass


class BpclNamespace(OrderedDict):
    def __str__(self):
        ret = []
        for k, v in self.items():
            ret.append('{} = {!r}'.format(k, v))
        return '\n'.join(ret)


def bins_from_string(bs, search_header=True):
    """

    :param bs:
    :param search_header:
    :return:
    :rtype: bitstring.BitStream
    """
    header = b'BUFR'
    if search_header:
        idx_header = bs.find(header)
        if idx_header != -1:
            bs = bs[bs.find(header):]
        else:
            raise BpclError('Cannot find starting signature: {}'.format(header))

    return bitstring.BitStream(bytes=bs)


def peek_edition(bins):
    """
    Peek the edition number from the input bits stream. The function does NOT
    advance the read position of the stream.

    :param bitstring.BitStream bins:
    :return:
    :rtype: int
    """
    return bins[56: 64].uint


def format_decoded_data(n_subsets,
                        decoded_descriptors_all_subsets,
                        decoded_values_all_subsets,
                        bitmap_links_all_subsets):
    """
    Format the data from section 4 so that they have an user-friend display.
    """
    ret = []
    for idx_subset in range(n_subsets):
        ret.append('###### subset {} of {} ######'.format(idx_subset + 1, n_subsets))
        descriptors = decoded_descriptors_all_subsets[idx_subset]
        bitmap_links = bitmap_links_all_subsets[idx_subset]
        values = decoded_values_all_subsets[idx_subset]
        for idx, (descriptor, value) in enumerate(itertools.izip(descriptors, values)):
            if value is not None and hasattr(descriptor, 'unit') and descriptor.unit == 'FLAG TABLE':
                value = (
                    value,
                    [(i + 1) for i, bit in enumerate(
                        '{:0{}b}'.format(value, descriptor.nbits)
                    ) if bit == '1']
                )

            if idx in bitmap_links:
                ret.append('{:4d} {:65.65} -> {:<6d} {!r}'.format(
                    idx + 1, descriptor.dumps(), bitmap_links[idx] + 1, value)
                )
            else:
                ret.append('{:4d} {:75.75} {!r}'.format(
                    idx + 1, descriptor.dumps(), value)
                )
    return ret
