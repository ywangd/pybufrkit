from __future__ import absolute_import
from __future__ import print_function

import six
import itertools
from pybufrkit.descriptors import flat_member_ids
from pybufrkit.templatedata import FixedReplicationNode, DelayedReplicationNode

__all__ = ['BufrTableDefinitionProcessor']


class BufrTableDefinitionProcessor(object):

    def process(self, bufr_message):
        assert bufr_message.n_subsets.value == 1, 'Expect only one subset for defining BUFR tables, got {}'.format(
            bufr_message.n_subsets.value)
        bufr_message.wire()
        template_data = bufr_message.template_data.value
        assert len(template_data.decoded_nodes) == 3, 'Expect 3 sections in template data for defining BUFR tables'
        next_value = self._process_table_a_entries(
            template_data.decoded_nodes[0], template_data.decoded_values)
        b_entries = self._process_table_b_entries(
            next_value, template_data.decoded_nodes[1], template_data.decoded_values)
        d_entries = self._process_table_d_entries(
            next_value, template_data.decoded_nodes[2], template_data.decoded_values)
        return [[], b_entries, d_entries]

    def _process_table_a_entries(self, decoded_node, decoded_values):
        n_repeats, is_delayed_replication = self._get_n_repeats(decoded_node, decoded_values)
        assert flat_member_ids(decoded_node.descriptor) == [1, 2, 3]
        vc = itertools.count(n_repeats * 3 + 1 if is_delayed_replication else 0)

        def get_decoded_values():
            value = decoded_values[next(vc)]
            if isinstance(value, six.binary_type):
                value = value.decode()
            return value

        return get_decoded_values

    def _process_table_b_entries(self, next_value, decoded_node, decoded_values):
        n_repeats, is_delayed_replication = self._get_n_repeats(decoded_node, decoded_values)
        if is_delayed_replication:
            next_value()

        assert flat_member_ids(decoded_node.descriptor) == [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        return dict([self._process_table_b_one_entry(next_value) for ir in range(n_repeats)])

    def _process_table_b_one_entry(self, next_value):
        return (
            next_value() + next_value() + next_value(),
            [
                next_value().rstrip() + next_value().rstrip(),
                next_value().strip(),
                (1 if next_value().strip() == '+' else -1) * int(next_value().strip()),
                (1 if next_value().strip() == '+' else -1) * int(next_value().strip()),
                int(next_value().strip()),
                '', 0, 0
            ]
        )

    def _process_table_d_entries(self, next_value, decoded_node, decoded_values):
        n_repeats, is_delayed_replication = self._get_n_repeats(decoded_node, decoded_values)
        if is_delayed_replication:
            next_value()

        assert flat_member_ids(decoded_node.descriptor) == [10, 11, 12, 205064, 101000, 31001, 30]

        return dict([self._process_table_d_one_entry(next_value) for _ in range(n_repeats)])

    def _process_table_d_one_entry(self, next_value):
        return (
            next_value() + next_value() + next_value(),
            [
                next_value().rstrip(),
                [next_value() for i in range(next_value())]
            ]
        )

    def _get_n_repeats(self, decoded_node, decoded_values):
        assert isinstance(decoded_node, (FixedReplicationNode, DelayedReplicationNode))
        if isinstance(decoded_node, FixedReplicationNode):
            return decoded_node.descriptor.n_repeats, False
        else:
            return decoded_values[decoded_node.factor.index], True
