"""
pybufrkit.templatedata
~~~~~~~~~~~~~~~~~~~~~~

The TemplateData object is dedicated to the data decoded for the template of a
BUFR message, while Bufr object is for the entire BUFR message. The object
provides a fully hierarchical view of the data with attributes properly
allocated to their corresponding values.
"""
from __future__ import absolute_import
from __future__ import print_function

import itertools
import functools

# noinspection PyUnresolvedReferences
from six.moves import range

from pybufrkit.errors import PyBufrKitError
from pybufrkit.descriptors import (ElementDescriptor,
                                   FixedReplicationDescriptor,
                                   DelayedReplicationDescriptor,
                                   OperatorDescriptor,
                                   SequenceDescriptor,
                                   SkippedLocalDescriptor)


class DataNode(object):
    """
    A node is composed of a descriptor and its value (if exists) and any
    possible child or attribute nodes.
    """

    def __init__(self, descriptor):
        self.descriptor = descriptor

    def __str__(self):
        ret = str(self.descriptor)
        if hasattr(self.descriptor, 'name'):
            ret += ' ' + self.descriptor.name
        return ret

    def __repr__(self):
        return str(self.descriptor)


# A node contains information that can be used to locate a descriptor with its associated values.
class NoValueDataNode(DataNode):
    """
    A no value node is for any descriptors that cannot have a value, e.g.
    replication descriptors, sequence descriptors and some operator descriptors,
    e.g. 201YYY.
    """

    def __init__(self, descriptor):
        super(NoValueDataNode, self).__init__(descriptor)


class FixedReplicationNode(NoValueDataNode):
    def __init__(self, descriptor):
        super(FixedReplicationNode, self).__init__(descriptor)
        self.members = []


class DelayedReplicationNode(NoValueDataNode):
    def __init__(self, descriptor):
        super(DelayedReplicationNode, self).__init__(descriptor)
        self.members = []
        self.factor = None


class SequenceNode(NoValueDataNode):
    def __init__(self, descriptor):
        super(SequenceNode, self).__init__(descriptor)
        self.members = []


class ValueDataNode(DataNode):
    """
    A value node is for any descriptors that can have a value attached to it.
    This includes all Element descriptor, Associated descriptor, Skipped local
    descriptor, some operator descriptors, e.g. 205YYY, 223255, etc.

    :param int index: The index to the descriptors and values array for getting the
                      descriptor and its associated value.
    """

    def __init__(self, descriptor, index):
        super(ValueDataNode, self).__init__(descriptor)
        self.index = index

    def __str__(self):
        return 'V{}'.format(self.index)

    def add_attribute(self, attr_node):
        # Add attributes field only when it is necessary
        if not hasattr(self, 'attributes'):
            self.attributes = [attr_node]
        else:
            self.attributes.append(attr_node)


# The following types of Node can be attributes
class AssociatedFieldNode(ValueDataNode):
    pass


class FirstOrderStatsNode(ValueDataNode):
    pass


class DifferenceStatsNode(ValueDataNode):
    pass


class SubstitutionNode(ValueDataNode):
    pass


class ReplacementNode(ValueDataNode):
    pass


class QualityInfoNode(ValueDataNode):
    pass


# noinspection PyAttributeOutsideInit
class TemplateData(object):
    """
    This class is dedicated to the data section of a BUFR message and produces a
    fully hierarchical structure for the otherwise flat list of decoded
    descriptors and values. Attributes like associated fields and statistical
    values are properly allocated to their corresponding referred elements.
    """

    def __init__(self,
                 template,
                 is_compressed,
                 decoded_descriptors_all_subsets,
                 decoded_values_all_subsets,
                 bitmap_links_all_subsets):

        self.template = template
        self.is_compressed = is_compressed
        self.decoded_descriptors_all_subsets = decoded_descriptors_all_subsets
        self.decoded_values_all_subsets = decoded_values_all_subsets
        self.bitmap_links_all_subsets = bitmap_links_all_subsets
        self.n_subsets = len(self.decoded_descriptors_all_subsets)

        # The most important difference between decoded nodes and decoded
        # descriptors is that the decoded descriptors are flat while decoded
        # nodes are nested. For an example, the replication is a single node
        # (with its own member nodes) in the decoded nodes but a series of flat
        # descriptors in decoded descriptors.

        if self.is_compressed:
            self.decoded_nodes_all_subsets = [[]] * self.n_subsets
        else:
            self.decoded_nodes_all_subsets = [[] for _ in range(self.n_subsets)]

        self.decoded_nodes = self.decoded_nodes_all_subsets[0]

        self._is_wired = False

    def wire(self):
        """
        From the flat list of descriptors and values, construct a fully
        hierarchical structure of data including sequence descriptors and
        correctly set bitmapped values to their corresponding node as
        attributes.
        """
        # Do not wire more than once
        if self._is_wired:
            return
        else:
            self._is_wired = True

        # For compressed data, the wiring is the same for all subsets.
        n_subsets = 1 if self.is_compressed else self.n_subsets

        for idx_subset in range(n_subsets):
            self.decoded_nodes = self.decoded_nodes_all_subsets[idx_subset]
            self.decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
            self.decoded_values = self.decoded_values_all_subsets[idx_subset]
            self.bitmap_links = self.bitmap_links_all_subsets[idx_subset]

            # The index is used to index into the decoded descriptors/values.
            # The is used to have links between flat indices and nested nodes,
            # so that attributes can be associated to their bit-mapped nodes.
            self.index_to_node = {}
            self.next_index = functools.partial(next, itertools.count())
            self.nbits_associated_list = []  # 204 YYY
            self.data_not_present_count = 0  # 221
            self.waiting_for_qa_info_meaning = False
            self.waiting_for_1st_order_stats_meaning = False
            self.waiting_for_difference_stats_meaning = False

            self.wire_members(self.template.members)

            # release memory
            del self.index_to_node

    def get_next_descriptor_and_index(self):
        index = self.next_index()
        return self.decoded_descriptors[index], index

    def add_node(self, node):
        self.decoded_nodes.append(node)
        if not isinstance(node, NoValueDataNode):
            self.index_to_node[node.index] = node
        return node

    def add_value_node(self):
        node = ValueDataNode(*self.get_next_descriptor_and_index())
        self.decoded_nodes.append(node)
        self.index_to_node[node.index] = node
        return node

    def add_delayed_replication_factor_node(self):
        factor_node = ValueDataNode(*self.get_next_descriptor_and_index())
        self.index_to_node[factor_node.index] = factor_node
        return factor_node

    def wire_element_descriptor(self, descriptor):
        # Read associated field if exists
        if self.nbits_associated_list and descriptor.X != 31:
            assoc_node = AssociatedFieldNode(*self.get_next_descriptor_and_index())
            assoc_node.add_attribute(self.associated_field_meaning)
            node = ValueDataNode(*self.get_next_descriptor_and_index())
            node.add_attribute(assoc_node)
            self.add_node(node)

        else:
            if descriptor.X == 33 and self.waiting_for_qa_info_meaning:
                node = self.add_node(QualityInfoNode(*self.get_next_descriptor_and_index()))
                self.index_to_node[self.bitmap_links[node.index]].add_attribute(node)

            else:
                node = self.add_value_node()
                if descriptor.id == 31021 and self.nbits_associated_list:
                    self.associated_field_meaning = node

                elif descriptor.id == 8023 and self.waiting_for_1st_order_stats_meaning:
                    self.first_order_stats_meaning = node
                    self.waiting_for_1st_order_stats_meaning = False

                elif descriptor.id == 8024 and self.waiting_for_difference_stats_meaning:
                    self.difference_stats_meaning = node
                    self.waiting_for_difference_stats_meaning = False

    def wire_fixed_replication_descriptor(self, descriptor):
        """
        :param FixedReplicationDescriptor descriptor:
        """
        fixed_replication_node = FixedReplicationNode(descriptor)
        nodes = self.decoded_nodes
        self.decoded_nodes = fixed_replication_node.members
        for _ in range(descriptor.n_repeats):
            self.wire_members(descriptor.members)
        self.decoded_nodes = nodes

        self.add_node(fixed_replication_node)

    def wire_delayed_replication_descriptor(self, descriptor):
        """
        :param DelayedReplicationDescriptor descriptor:
        """
        delayed_replication_node = DelayedReplicationNode(descriptor)
        nodes = self.decoded_nodes
        self.decoded_nodes = delayed_replication_node.members

        # Add the delayed replication factor node to the node indices as well as
        # it is possible to have attributes attached to it. For an example,
        # ocea_133.bufr from benchmark data has QA info attached to 031001.
        factor_node = self.add_delayed_replication_factor_node()
        delayed_replication_node.factor = factor_node
        for _ in range(self.decoded_values[factor_node.index]):
            self.wire_members(descriptor.members)
        self.decoded_nodes = nodes

        self.add_node(delayed_replication_node)

    def wire_sequence_descriptor(self, descriptor):
        sequence_node = SequenceNode(descriptor)
        nodes = self.decoded_nodes
        self.decoded_nodes = sequence_node.members
        self.wire_members(descriptor.members)
        self.decoded_nodes = nodes

        self.add_node(sequence_node)

    def wire_bitmap_attribute(self, attr_node):
        self.index_to_node[self.bitmap_links[attr_node.index]].add_attribute(attr_node)

    def wire_operator_descriptor(self, descriptor):
        """
        :param OperatorDescriptor descriptor:
        :return:
        """
        operator_code, operand_value = descriptor.operator_code, descriptor.operand_value

        if operator_code in (201, 202, 203, 206, 207, 208,):
            # nbits offset, scale offset, new refval, skip local, increment, change string length
            self.add_node(NoValueDataNode(descriptor))

        elif operator_code == 204:  # associated field
            if operand_value == 0:
                self.nbits_associated_list.pop()
            else:
                self.nbits_associated_list.append(operand_value)
            self.add_node(NoValueDataNode(descriptor))

        elif operator_code == 205:  # read string of YYY bytes
            self.add_value_node()

        # Data not present for following YYY descriptors except class 0-9 and 31
        elif operator_code == 221:
            self.data_not_present_count = operand_value
            self.add_node(NoValueDataNode(descriptor))

        elif operator_code == 222:  # quality info follows
            self.waiting_for_qa_info_meaning = True
            self.add_value_node()

        elif operator_code == 223:  # substituted value
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.add_value_node()
            else:
                node = self.add_node(SubstitutionNode(*self.get_next_descriptor_and_index()))
                self.wire_bitmap_attribute(node)

        elif operator_code == 224:  # 1st order stats
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.waiting_for_1st_order_stats_meaning = True
                self.add_value_node()
            else:
                node = self.add_node(FirstOrderStatsNode(*self.get_next_descriptor_and_index()))
                node.add_attribute(self.first_order_stats_meaning)
                self.wire_bitmap_attribute(node)

        elif operator_code == 225:  # difference stats
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.waiting_for_difference_stats_meaning = True
                self.add_value_node()
            else:
                node = self.add_node(DifferenceStatsNode(*self.get_next_descriptor_and_index()))
                node.add_attribute(self.difference_stats_meaning)
                self.wire_bitmap_attribute(node)

        elif operator_code == 232:  # replaced/retained value
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.add_value_node()
            else:
                node = self.add_node(ReplacementNode(*self.get_next_descriptor_and_index()))
                self.wire_bitmap_attribute(node)

        elif operator_code == 235:  # cancel all backwards data reference
            self.waiting_for_qa_info_meaning = False
            self.add_node(NoValueDataNode(descriptor))

        elif operator_code == 236:
            self.add_value_node()

        elif operator_code == 237:
            self.add_value_node()

        else:  # TODO: 241, 242, 243
            raise NotImplemented('Operator Descriptor {} not implemented'.format(descriptor))

    def wire_skippable_local_descriptor(self):
        self.add_value_node()

    def wire_members(self, members):
        for member in members:

            # 221 YYY data not present for following YYY descriptors except class 0-9 and 31
            if self.data_not_present_count:
                self.data_not_present_count -= 1
                if isinstance(member, ElementDescriptor):
                    X = member.X
                    if not (1 <= X <= 9 or X == 31):  # skipping
                        self.add_node(NoValueDataNode(member))
                        continue

            # Now process normally
            if isinstance(member, ElementDescriptor):
                self.wire_element_descriptor(member)

            elif isinstance(member, FixedReplicationDescriptor):
                self.wire_fixed_replication_descriptor(member)

            elif isinstance(member, DelayedReplicationDescriptor):
                self.wire_delayed_replication_descriptor(member)

            elif isinstance(member, OperatorDescriptor):
                self.wire_operator_descriptor(member)

            elif isinstance(member, SequenceDescriptor):
                self.wire_sequence_descriptor(member)

            elif isinstance(member, SkippedLocalDescriptor):
                self.wire_skippable_local_descriptor()

            else:
                raise PyBufrKitError('Cannot wire descriptor type: {}'.format(type(member)))
