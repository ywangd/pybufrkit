"""
The BufrData object is dedicated to data from section 4 of the BUFR message,
while `pybufrkit.bufr.Bufr` is for the entire BUFR message. The object provides
a fully hierarchical view of the data with attributes properly allocated to
their corresponding values.
"""
from __future__ import absolute_import
import itertools
import weakref
import functools

import pyparsing as pp

from .descriptors import (ElementDescriptor, FixedReplicationDescriptor,
                          DelayedReplicationDescriptor, OperatorDescriptor,
                          SequenceDescriptor, AssociatedDescriptor,
                          SkippedLocalDescriptor,
                          MarkerDescriptor)
from .descriptorsplus import VirtualAttributedDescriptor

from .utils import BpclError, INDENT_CHARS
from six.moves import range


class NoValueNode(object):
    """
    A no value node is for any descriptors that cannot have a value, e.g.
    replication descriptors, sequence descriptors and some operator descriptors,
    e.g. 201YYY.
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


class FixedReplicationNode(NoValueNode):
    def __init__(self, descriptor):
        super(FixedReplicationNode, self).__init__(descriptor)
        self.members = []


class DelayedReplicationNode(NoValueNode):
    def __init__(self, descriptor):
        super(DelayedReplicationNode, self).__init__(descriptor)
        self.members = []
        self.factor = None


class SequenceNode(NoValueNode):
    def __init__(self, descriptor):
        super(SequenceNode, self).__init__(descriptor)
        self.members = []


class ValueNode(object):
    """
    A value node is for any descriptors that can have a value attached to it.
    This includes all Element descriptor, Associated descriptor, Skipped local
    descriptor, some operator descriptors, e.g. 205YYY, 223255, etc.
    """
    def __init__(self, index):
        self.index = index
        self.attributes = []

    def __str__(self):
        return 'V{}'.format(self.index)

    def add_attribute(self, attr_node):
        self.attributes.append(attr_node)


# The following types of Node can be attributes
class AssociatedFieldNode(ValueNode):
    pass


class FirstOrderStatsNode(ValueNode):
    pass


class DifferenceStatsNode(ValueNode):
    pass


class SubstitutionNode(ValueNode):
    pass


class ReplacementNode(ValueNode):
    pass


class QualityInfoNode(ValueNode):
    pass


def subscript_action(s, loc, tokens):
    if 'slice' in tokens:
        start = int(tokens.slice.start[0]) if 'start' in tokens.slice else None
        stop = int(tokens.slice.stop[0]) if 'stop' in tokens.slice else None
        step = int(tokens.slice.step[0]) if 'step' in tokens.slice else None
        return slice(start, stop, step)
    else:
        return int(tokens[0])


def count_action(s, loc, tokens):
    index = int(tokens[0])
    if 'subscript' in tokens:
        return index, tokens.subscript[0]
    else:
        return index, None


class PositionStringParser(object):
    """
    Parser for position description strings. A position description string is
    composed of a series of positions pointing to one descriptor in a BUFR
    template.

    This single descriptor can however corresponds to multiple nodes
    in an object of BufrData. For an example, a descriptor directly or
    indirectly enclosed by a replication descriptor can correspond to multiple
    nodes.

    The positions are 1-based indices with the exception of the position of
    delayed replication factor, which is always Zero.

    Positions are used to index subsets, descriptor nodes, and attribute nodes.
    Positions are separated by comma with the exception of attribute nodes which
    are separated by dot.

    An example of position description strings:
        #1, 3, 1, 4.2.1
        The above means subset 1, descriptor 3, sub-descriptor 1,
        sub-sub-descriptor 4 and attribute 2 of sub-sub-descriptor 4 and
        sub-attribute 1 of attribute 2. Following diagram shows the hierarchical
        view of the position string.

        #1 (subset)
        --3  (descriptor)
        ----1  (sub-descriptor)
        ------4  (sub-sub-descriptor)
        --------2  (attribute)
        ----------1  (sub-attribute)

    Note the subset position is prefixed with a hash character. It can also be
    omitted with a default value of One. So the above string is equal to
    3, 1, 4.2.1

    It is also possible to specify additional slice for a position if the
    pointed descriptor corresponds to multiple nodes. An example of position string
    including slice is as follows:
        3, 1, 2[::2].1
    The above position string requires the corresponding nodes at position 3, 1, 2
    to be sliced by [::2] (every other nodes). The resulted nodes are then processed
    further to get the attribute 1.
    """
    def __init__(self):
        comma = pp.Literal(',')
        minus = pp.Literal('-')
        lbracket = pp.Literal('[')
        rbracket = pp.Literal(']')
        colon = pp.Literal(':')

        subset_specifier = pp.Literal('#').suppress() + pp.Word(pp.nums) + comma.suppress()
        index = pp.Optional(minus) + pp.Word(pp.nums)
        subscript = (
            index ^
            (
                pp.Optional(index)('start') + colon.suppress() +
                pp.Optional(index)('stop') +
                pp.Optional(colon.suppress() + pp.Optional(index)('step'))
            )('slice')
        ).setParseAction(subscript_action)

        count = (
            pp.Word(pp.nums) +
            pp.Optional(lbracket.suppress() + subscript + rbracket.suppress())('subscript')
        ).setParseAction(count_action)

        descriptor_specifier = pp.ZeroOrMore(count + comma.suppress()) + count

        attribute_specifier = pp.Literal('.').suppress() + count

        self.parser = (
            pp.Optional(subset_specifier) +
            pp.Group(descriptor_specifier) +
            pp.Group(pp.ZeroOrMore(attribute_specifier))
        )

    def parse(self, position_string):
        """
        Parse the position descriptor string for position information to be used
        for query.

        The return value is a 3-member tuple. First element is the position of
        subset. Second element is a list of descriptor specifiers. Third element
        is a list of attribute specifiers.

        A descriptor specifier is a 2-member tuple with first element being
        position of the descriptor and the second element being a slice object
        or None if no slice required.

        An attribute specifier is similar to a descriptor specifier with first
        element being the position of attribute and 2nd element being a slice
        object or None.

        :param position_string: A position descriptor string.
        """
        parsed = self.parser.parseString(position_string, parseAll=True)
        if len(parsed) == 2:
            return 1, tuple(parsed[0]), tuple(parsed[1])
        else:
            return int(parsed[0]), tuple(parsed[1]), tuple(parsed[2])


position_string_parser = PositionStringParser()


def parse_position_string(position_string):
    return position_string_parser.parse(position_string)


def name_specifier_action(s, loc, tokens):
    if 'subscript' in tokens:
        return tokens[0], tokens.subscript[0]
    else:
        return tokens[0], None


class NameStringParser(object):
    """
    Parser for name strings. A name string is composed of the name of an element
    descriptor and optionally followed by attribute specifier (also with names).

    The name string is similar to position string. The differences are:
        1. Cannot specify subset, the query is always for all subsets.
        2. Can only specify the name of a single Element descriptor with no
           means to require the descriptor context (i.e. its ancestor descriptors)
        3. Attributes can be specified for the Element descriptor. But they
           must be specified by name as well.

    Names are by default the string representation of descriptors. For regular
    element descriptor, this is the ID, e.g. 001001 for WMO block number. Note
    it must be written in the six digit form, i.e. with the leading Zeros. For
    an example:
        001001.A01001
    The above name string is to query for the associated filed (A01001) for
    element descriptor 001001.

    Slice can also be specified to names similar to position string.
    """
    def __init__(self):
        minus = pp.Literal('-')
        lbracket = pp.Literal('[')
        rbracket = pp.Literal(']')
        colon = pp.Literal(':')

        name = pp.Word(pp.alphanums + '$', pp.alphanums + '_$')

        index = pp.Optional(minus) + pp.Word(pp.nums)
        subscript = (
            index ^
            (
                pp.Optional(index)('start') + colon.suppress() +
                pp.Optional(index)('stop') +
                pp.Optional(colon.suppress() + pp.Optional(index)('step'))
            )('slice')
        ).setParseAction(subscript_action)

        name_specifier = (
            name +
            pp.Optional(lbracket.suppress() + subscript + rbracket.suppress())('subscript')
        ).setParseAction(name_specifier_action)

        attribute_specifier = pp.Literal('.').suppress() + name_specifier

        self.parser = (
            pp.Group(name_specifier) +
            pp.Group(pp.ZeroOrMore(attribute_specifier))
        )

    def parse(self, name_string):
        parsed = self.parser.parseString(name_string, parseAll=True)
        return tuple(parsed[0][0]), tuple(parsed[1])


name_string_parser = NameStringParser()


def parse_name_string(name_string):
    return name_string_parser.parse(name_string)


def walk_decoded_nodes(decoded_nodes):
    """
    Recursively walk the given list of decoded nodes.

    :param [NoValueNode | ValueNode] decoded_nodes: A list of decoded nodes
    """
    for node in decoded_nodes:
        if isinstance(node, ValueNode):
            yield node
        else:
            if hasattr(node, 'members'):
                for sub_node in walk_decoded_nodes(node.members):
                    yield sub_node


# noinspection PyAttributeOutsideInit
class BufrData(object):
    """
    This class is dedicated to the data section of a BUFR message and produces a
    fully hierarchical structure for the otherwise flat list of decoded
    descriptors and values. Attributes like associated fields and statistical
    values are properly allocated under their corresponding referred elements.

    It also provides interfaces to query the data using either positions or
    names.
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

        if self.is_compressed:
            self.decoded_nodes_all_subsets = [[]] * self.n_subsets
            self.decoded_nodes = self.decoded_nodes_all_subsets[0]
        else:
            self.decoded_nodes_all_subsets = [[] for _ in range(self.n_subsets)]

        self.wire()

    def query_by_position(self, position_string):
        """
        Get value(s) using a position string. A position string is a set of
        pointers pointing to the required element(s). The pointers are the
        1-based positions of subset and descriptors.
        """
        return self._query_by_position(*parse_position_string(position_string))

    def query_by_name(self, name_string):
        """
        Get values(s) with a name string. A name string is the name/ID of a
        element descriptor plus possible attribute name/ID.
        """
        return self._query_by_name(*parse_name_string(name_string))

    def dumps(self, indent=''):
        """
        Dump the object data in a hierarchical format.
        """
        ret = []
        for idx_subset in range(self.n_subsets):
            self.decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
            self.decoded_values = self.decoded_values_all_subsets[idx_subset]
            ret.append('###### subset {} of {} ######'.format(idx_subset + 1, self.n_subsets))
            ret.extend(self._dumps(self.decoded_nodes_all_subsets[idx_subset], indent))

        return '\n'.join(ret)

    def structure_view(self, indent=''):
        """
        This method is similar to `pybufrkit.descriptorsplus.dumps`. The
        advantage of this method is that the attributes are listed under their
        corresponding element descriptors as well as their original positions.
        Also note that some descriptors may be MISSING from the view because it
        is derived from an actual decoded message. So if a descriptor is under a
        delayed replication and the replication factor is Zero. The descriptor
        will be shown in the structure.

        In general, this method should be used in conjunction with the dumps
        function depending on the situation.
        """
        ret = self._structure_view(self.decoded_nodes_all_subsets[0], indent)
        return '\n'.join(ret)

    def _query_by_position(self, count_subset, descriptor_specifiers, attribute_specifiers=()):

        idx_subset = count_subset - 1
        decoded_nodes = self.decoded_nodes_all_subsets[idx_subset]
        decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
        decoded_values = self.decoded_values_all_subsets[idx_subset]

        count, slc = descriptor_specifiers[0]
        assert slc is None, 'Can only slice descriptors enclosed by replications'
        parent_nodes = [decoded_nodes[count - 1]]

        for count, slc in descriptor_specifiers[1:]:
            child_nodes = []
            for parent_node in parent_nodes:
                if count == 0:
                    assert isinstance(parent_node, DelayedReplicationNode), \
                        'Index 0 is only for delayed replication factor'
                    child_nodes.append(parent_node.factor)

                elif isinstance(parent_node, (FixedReplicationNode, DelayedReplicationNode)):
                    if isinstance(parent_node, FixedReplicationNode):
                        n_repeats = parent_node.descriptor.n_repeats
                    else:
                        n_repeats = decoded_values[parent_node.factor.index]
                    n_members = parent_node.descriptor.n_members
                    for ir in range(n_repeats):
                        child_nodes.append(parent_node.members[ir * n_members + count - 1])

                else:
                    child_nodes.append(parent_node.members[count - 1])

            if slc:
                parent_nodes = [child_nodes[slc]] if isinstance(slc, int) else child_nodes[slc]
            else:
                parent_nodes = child_nodes

        for count, slc in attribute_specifiers:
            child_nodes = []
            for parent_node in parent_nodes:
                child_nodes.append(parent_node.attributes[count - 1])

            if slc:
                parent_nodes = [child_nodes[slc]] if isinstance(slc, int) else child_nodes[slc]
            else:
                parent_nodes = child_nodes

        descriptors = [decoded_descriptors[node.index] for node in parent_nodes]
        values = [decoded_values[node.index] for node in parent_nodes]

        if len(descriptors) == 1:
            return descriptors[0], values[0]
        else:
            return descriptors, values

    def _query_by_name(self, name_specifier, attribute_specifiers=()):
        name, slc = name_specifier
        if name.startswith('$'):
            name = name[1:]

        attr_specs = []
        for attr_name, attr_slc in attribute_specifiers:
            if attr_name.startswith('$'):
                attr_name = attr_name[1:]
            attr_specs.append((attr_name, attr_slc))

        descriptors_all_subsets = []
        values_all_subsets = []
        for idx_subset in range(self.n_subsets):
            decoded_nodes = self.decoded_nodes_all_subsets[idx_subset]
            decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
            decoded_values = self.decoded_values_all_subsets[idx_subset]

            nodes = []
            for node in walk_decoded_nodes(decoded_nodes):
                if name == str(decoded_descriptors[node.index]):
                    nodes.append(node)

            if slc is not None:
                nodes = [nodes[slc]] if isinstance(slc, int) else nodes[slc]

            node = nodes[0]
            for attr_name, attr_slc in attr_specs:
                attr_idx = self._query_by_name_find_attr(decoded_descriptors, node, attr_name)
                nodes = [node.attributes[attr_idx] for node in nodes]
                if attr_slc is not None:
                    nodes = [nodes[attr_slc]] if isinstance(slc, int) else nodes[attr_slc]
                node = nodes[0]

            descriptors_all_subsets.append([decoded_descriptors[node.index] for node in nodes])
            values_all_subsets.append([decoded_values[node.index] for node in nodes])

        return descriptors_all_subsets, values_all_subsets

    def _query_by_name_find_attr(self, decoded_descriptors, node, attr_name):
        for idx, attr_node in enumerate(node.attributes):
            if str(decoded_descriptors[attr_node.index]) == attr_name:
                return idx

    def wire(self):
        """
        From the flat list of descriptors and values, construct a fully
        hierarchical structure of data including sequence descriptors and
        correctly set bitmapped values to their corresponding node as
        attributes.
        """
        n_subsets = 1 if self.is_compressed else self.n_subsets

        for idx_subset in range(n_subsets):
            self.decoded_nodes = self.decoded_nodes_all_subsets[idx_subset]
            self.decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
            self.decoded_values = self.decoded_values_all_subsets[idx_subset]
            self.bitmap_links = self.bitmap_links_all_subsets[idx_subset]

            self.index_to_node = weakref.WeakValueDictionary()
            self.next_index = functools.partial(next, itertools.count())
            self.nbits_associated_list = []  # 204 YYY
            self.data_not_present_count = 0  # 221
            self.waiting_for_qa_info_meaning = False
            self.waiting_for_1st_order_stats_meaning = False
            self.waiting_for_difference_stats_meaning = False

            self.wire_members(self.template.members)

    def _dumps(self, nodes, indent):
        ret = []
        for node in nodes:
            if isinstance(node, NoValueNode):
                ret.append('{}{}'.format(indent, node))

                if isinstance(node, SequenceNode):
                    ret.extend(self._dumps(node.members, indent + INDENT_CHARS))

                elif isinstance(node, (FixedReplicationNode, DelayedReplicationNode)):
                    if isinstance(node, FixedReplicationNode):
                        n_repeats = node.descriptor.n_repeats
                    else:
                        n_repeats = self.decoded_values[node.factor.index]
                        ret.extend(self._dumps_value_node(node.factor, indent + '.' * len(INDENT_CHARS)))

                    # Get actual number of members instead of number of items which is
                    # calculated from the descriptor ID. When the structure is fully
                    # nested. The number from the descriptor ID is no longer accurate.
                    n_members = node.descriptor.n_members

                    for ir in range(n_repeats):
                        ret.append('{}# --- {} of {} replications ---'.format(
                            indent + INDENT_CHARS, ir + 1, n_repeats)
                        )
                        ret.extend(
                            self._dumps(node.members[ir * n_members: (ir + 1) * n_members],
                                        indent + INDENT_CHARS)
                        )

            else:  # ValueNode
                ret.extend(self._dumps_value_node(node, indent))

        return ret

    def _dumps_value_node(self, node, indent, is_attribute=False):
        descriptor = self.decoded_descriptors[node.index]
        value = self.decoded_values[node.index]
        if isinstance(descriptor, MarkerDescriptor):
            description = '{:06d}'.format(descriptor.marker_id)
        elif hasattr(descriptor, 'name'):
            description = descriptor.name
        else:
            description = node.__class__.__name__[:-4]

        ret = [
            '{}{}{} {} {!r}'.format(
                indent,
                '-> ' if is_attribute else '',
                descriptor,
                description,
                value
            )
        ]
        ret.extend(self._dumps_attr_nodes(node, indent + INDENT_CHARS))
        return ret

    def _dumps_attr_nodes(self, node, indent):
        ret = []
        for attr_node in node.attributes:
            ret.extend(self._dumps_value_node(attr_node, indent, True))
        return ret

    def _structure_view(self, nodes, indent):
        ret = []
        for idx, node in enumerate(nodes):
            if isinstance(node, NoValueNode):
                ret.append('{}[{}] {}'.format(indent, idx + 1, node))

                if isinstance(node, SequenceNode):
                    ret.extend(self._structure_view(node.members, indent + INDENT_CHARS))

                else:
                    if isinstance(node, (FixedReplicationNode, DelayedReplicationNode)):
                        if isinstance(node, DelayedReplicationNode):
                            ret.extend(
                                self._structure_view_value_node(
                                    node.factor, 0, indent + '.' * len(INDENT_CHARS))
                            )

                        ret.extend(
                            self._structure_view(
                                node.members[:node.descriptor.n_members], indent + INDENT_CHARS
                            )
                        )
            else:
                ret.extend(self._structure_view_value_node(node, idx + 1, indent))

        return ret

    def _structure_view_value_node(self, node, count, indent, is_attribute=False):
        descriptor = self.decoded_descriptors[node.index]
        if isinstance(descriptor, MarkerDescriptor):
            description = '{:06d}'.format(descriptor.marker_id)
        elif hasattr(descriptor, 'name'):
            description = descriptor.name
        else:
            description = node.__class__.__name__[:-4]

        ret = [
            '{}[{}] {}{} {}'.format(
                indent,
                count,
                '-> ' if is_attribute else '',
                descriptor,
                description,
            )
        ]
        ret.extend(self._structure_view_attr_nodes(node, indent + INDENT_CHARS))
        return ret

    def _structure_view_attr_nodes(self, node, indent):
        ret = []
        for idx, attr_node in enumerate(node.attributes):
            ret.extend(self._structure_view_value_node(
                attr_node, idx + 1, indent, True))
        return ret

    def add_node(self, node):
        self.decoded_nodes.append(node)
        if not isinstance(node, NoValueNode):
            self.index_to_node[node.index] = node
        return node

    def add_value_node(self):
        return self.add_node(ValueNode(self.next_index()))

    def wire_element_descriptor(self, descriptor):
        # Read associated field if exists
        if self.nbits_associated_list and descriptor.X != 31:
            assoc_node = AssociatedFieldNode(self.next_index())
            assoc_node.add_attribute(self.associated_field_meaning)
            node = ValueNode(self.next_index())
            node.add_attribute(assoc_node)
            self.add_node(node)

        else:
            if descriptor.X == 33 and self.waiting_for_qa_info_meaning:
                node = self.add_node(QualityInfoNode(self.next_index()))
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
        factor_node = ValueNode(self.next_index())
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
            self.add_node(NoValueNode(descriptor))

        elif operator_code == 204:  # associated field
            if operand_value == 0:
                self.nbits_associated_list.pop()
            else:
                self.nbits_associated_list.append(operand_value)
            self.add_node(NoValueNode(descriptor))

        elif operator_code == 205:  # read string of YYY bytes
            self.add_value_node()

        # Data not present for following YYY descriptors except class 0-9 and 31
        elif operator_code == 221:
            self.data_not_present_count = operand_value
            self.add_node(NoValueNode(descriptor))

        elif operator_code == 222:  # quality info follows
            self.waiting_for_qa_info_meaning = True
            self.add_value_node()

        elif operator_code == 223:  # substituted value
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.add_value_node()
            else:
                node = self.add_node(SubstitutionNode(self.next_index()))
                self.wire_bitmap_attribute(node)

        elif operator_code == 224:  # 1st order stats
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.waiting_for_1st_order_stats_meaning = True
                self.add_value_node()
            else:
                node = self.add_node(FirstOrderStatsNode(self.next_index()))
                node.add_attribute(self.first_order_stats_meaning)
                self.wire_bitmap_attribute(node)

        elif operator_code == 225:  # difference stats
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.waiting_for_difference_stats_meaning = True
                self.add_value_node()
            else:
                node = self.add_node(DifferenceStatsNode(self.next_index()))
                node.add_attribute(self.difference_stats_meaning)
                self.wire_bitmap_attribute(node)

        elif operator_code == 232:  # replaced/retained value
            self.waiting_for_qa_info_meaning = False
            if operand_value == 0:
                self.add_value_node()
            else:
                node = self.add_node(ReplacementNode(self.next_index()))
                self.wire_bitmap_attribute(node)

        elif operator_code == 235:  # cancel all backwards data reference
            self.waiting_for_qa_info_meaning = False
            self.add_node(NoValueNode(descriptor))

        elif operator_code == 236:
            self.add_value_node()

        elif operator_code == 237:
            self.add_value_node()

        else:  # TODO: 241, 242, 243
            pass

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
                        self.add_node(NoValueNode(member))
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

            elif isinstance(member, VirtualAttributedDescriptor):
                # NOTE that the attributes are wired elsewhere by the actual descriptors
                self.wire_element_descriptor(member.descriptor)

            else:
                raise BpclError('Cannot wire descriptor type: {}'.format(type(member)))
