"""
Virtual Descriptors and Template are helper classes to manipulate BUFR
Templates. Virtual Descriptors can be inserted into and removed from a Template
without changing its fully expanded form. A Virtual Template has all of its
sequence descriptors and fixed replications as Virtual.

The classes are mainly designed to help mapping between a BUFR Template and a
NEONS Sequence. A smoother and simpler mapping mechanism can be created between
virtual Template and Sequence because they can be modified to have similar
structure. The data exchange happens on the Virtual layer. Then the Virtual
Sequence can persist the data into database by reducing itself to the actual
Sequence which directly relates to the database.
"""
from __future__ import absolute_import
import pyparsing as pp

from .descriptors import (DescriptorBase,
                          ElementDescriptor, SkippedLocalDescriptor,
                          MarkerDescriptor, OperatorDescriptor, AssociatedDescriptor,
                          FixedReplicationDescriptor,
                          DelayedReplicationDescriptor,
                          SequenceDescriptor, BufrTemplate)
from .utils import BpclError, INDENT_CHARS
from six.moves import range

__all__ = ['VirtualBufrTemplate']


class VirtualFixedReplicationDescriptor(FixedReplicationDescriptor):
    @staticmethod
    def from_fixed_replication_descriptor(fixed_replication_descriptor, including_fixed_replication):
        members = _virtualise_members(fixed_replication_descriptor.members, including_fixed_replication)
        return VirtualFixedReplicationDescriptor(
            fixed_replication_descriptor.id,
            members
        )

    def _serialize(self, indent, count):
        lines = [
            '{}{} {} {}  # [{}]'.format(
                indent, 'VirtualFixedReplication', self.n_repeats, '{', count
            )
        ]
        for idx, member in enumerate(self.members):
            lines.extend(member._serialize(indent + INDENT_CHARS, idx + 1))
        lines.append('{}{}'.format(indent, '}'))
        return lines


class VirtualSequenceDescriptor(SequenceDescriptor):
    @staticmethod
    def from_sequence_descriptor(sequence_descriptor, including_fixed_replication):
        members = _virtualise_members(sequence_descriptor.members, including_fixed_replication)
        return VirtualSequenceDescriptor(
            sequence_descriptor.id,
            sequence_descriptor.name,
            members
        )

    def _serialize(self, indent, count):
        lines = ['{}{}  # [{}]{}'.format(indent, 'VirtualSequence', count, self.name)]
        for idx, member in enumerate(self.members):
            lines.extend(member._serialize(indent + INDENT_CHARS, idx + 1))
        lines.append('{}{}'.format(indent, '}'))
        return lines


class VirtualAttributedDescriptor(DescriptorBase):
    """
    NOTE this class is different from AssociatedDescriptor, MarkerDescriptor in
    that the latter physically correspond to values in the binary data stream
    and in that order. This descriptor is purely a conceptual placeholder of the
    attributes. Its attributes are NOT processed when wiring the BUFR data.
    Instead, the attributes are wired when the actual descriptors are processed.
    """

    def __init__(self, descriptor, attributes=None):
        super(VirtualAttributedDescriptor, self).__init__(0)
        self.descriptor = descriptor
        self.attributes = attributes

    def _serialize(self, indent, count):
        lines = ['{}{} {}  # [{}]'.format(
            indent, self.descriptor, '{', count)]
        for idx, attr in enumerate(self.attributes):
            lines.extend(attr._serialize(indent + INDENT_CHARS, idx + 1))
        lines += ['{}{}'.format(indent, '}')]
        return lines


class VirtualBufrTemplate(BufrTemplate):
    """
    When inserting and removing virtual descriptors, any enclosing replication
    descriptors will NOT change their IDs. This means, for Virtual Template, it
    is NOT accurate to count the actual number of members to be replicated by
    looking at the replication descriptor ID. The number of members to be
    replicated should always be obtained by checking the actual length of the
    member property of a replication descriptor.
    """

    def __init__(self, id_, name, members=None):
        super(VirtualBufrTemplate, self).__init__(id_, name, members)

    @staticmethod
    def from_actual_template(template, name=None, including_fixed_replication=True):
        members = _virtualise_members(template.members, including_fixed_replication)
        return VirtualBufrTemplate(
            template.id,
            name or template.name or 'NONAME',
            members
        )

    def dumps(self):
        return dumps_virtual_descriptor(self)

    def serialize(self):
        lines = ['# {}'.format(self),
                 '{} {}'.format(self.name, '{')]
        for idx, member in enumerate(self.members):
            lines.extend(member._serialize(indent='', count=idx + 1))
        lines.append('}')
        return '\n'.join(lines)

    def insert_virtual_sequence_descriptor(self, n_members, *position):
        self._insert_virtual_sequence_descriptor(self, list(position), n_members)

    def insert_virtual_fixed_replication_descriptor(self, n_members, n_repeats, *position):
        """

        :param position: A list/tuple of 1-based indices that points to where the
                     virtual fixed replication should be inserted.
        :param n_members:
        :param n_repeats:
        :return:
        """
        # TODO: check whether the replication is logical, i.e. each set of items are the same
        self._insert_virtual_fixed_replication_descriptor(self, list(position), n_members, n_repeats)

    def insert_virtual_attributed_descriptor(self, n_members, *position):
        """
        This method is unlikely to be useful. This is because this method
        expects the attributes to be listed right after the master element,
        while the actual template does not provide such structure (it is done
        with bitmaps). So it is only usable for an already transformed template
        where the attributes are re-allocated.

        In addition, the attributes are wire automatically when building the
        BufrData object. Hence the virtual attributes are ignored even if they
        exists (see `wire_members` method of `BufrData`)

        The virtual Attributed descriptor is mainly used to render the mapping
        between NEONS and BUFR, though the mapping would still work without
        it as the actual attributes are generated during BUFR data wiring.
        """
        self._insert_virtual_attributed_descriptor(self, list(position), n_members)

    def remove_virtual_descriptor(self, *position):
        """
        Remove a virtual descriptor from the give position.
        """
        self._remove_virtual_descriptor(self, list(position))

    def remove_all_virtual_sequence_descriptors(self):
        """
        Remove all virtual sequence descriptors recursively from the Template.
        """
        self._remove_all_virtual_descriptors_of_type(
            self.members, [], VirtualSequenceDescriptor)

    def _insert_virtual_fixed_replication_descriptor(self, descriptor, position, n_members, n_repeats):
        idx = position.pop(0) - 1
        if len(position) == 0:
            members = [descriptor.members.pop(idx) for _ in range(n_members)]
            for ir in range(n_repeats - 1):
                assert members == [descriptor.members.pop(idx) for _ in range(n_members)]
            vfrd = VirtualFixedReplicationDescriptor(
                100000 + n_members * 1000 + n_repeats, members)
            descriptor.members.insert(idx, vfrd)

        else:
            descriptor = descriptor.members[idx]
            self._insert_virtual_fixed_replication_descriptor(descriptor, position, n_members, n_repeats)

    def _insert_virtual_sequence_descriptor(self, descriptor, position, n_members):
        idx = position.pop(0) - 1
        if len(position) == 0:  # We are now in position to insert
            members = []
            for _ in range(n_members):
                members.append(descriptor.members.pop(idx))
            vsd = VirtualSequenceDescriptor(300000, '', members)
            descriptor.members.insert(idx, vsd)

        else:  # go deeper along the position indices
            self._insert_virtual_sequence_descriptor(descriptor.members[idx], position, n_members)

    def _insert_virtual_attributed_descriptor(self, descriptor, position, n_members):
        idx = position.pop(0) - 1
        if len(position) == 0:  # We are now at position to insert
            parm = descriptor.members.pop(idx)
            attributes = []
            for _ in range(n_members - 1):
                attributes.append(descriptor.members.pop(idx))
            vad = VirtualAttributedDescriptor(parm, attributes)
            descriptor.members.insert(idx, vad)

        else:  # go deeper along the position indices
            self._insert_virtual_attributed_descriptor(
                descriptor.members[idx], position, n_members)

    def _remove_virtual_descriptor(self, descriptor, position):
        idx = position.pop(0) - 1
        if len(position) == 0:  # We are now at position to remove the virtual descriptor
            des = descriptor.members.pop(idx)
            if isinstance(des, VirtualSequenceDescriptor):
                for member in reversed(des.members):
                    descriptor.members.insert(idx, member)
            elif isinstance(des, VirtualFixedReplicationDescriptor):
                for _ in range(des.n_repeats):
                    for member in reversed(des.members):
                        descriptor.members.insert(idx, member)
            elif isinstance(des, VirtualAttributedDescriptor):
                for attr in reversed(des.attributes):
                    descriptor.members.insert(idx, attr)
                descriptor.members.insert(idx, des.descriptor)
            else:
                raise BpclError('Cannot remove non-virtual descriptor: {}'.format(des))
        else:
            self._remove_virtual_descriptor(descriptor.members[idx], position)

    def _remove_all_virtual_descriptors_of_type(self, members, position_list, descriptor_type):
        for idx_member in range(len(members) - 1, -1, -1):
            member = members[idx_member]
            if isinstance(member, (SequenceDescriptor,
                                   FixedReplicationDescriptor,
                                   DelayedReplicationDescriptor)):
                new_position_list = position_list + [idx_member + 1]
                # Remove any virtual sequences that are descendants of this member
                self._remove_all_virtual_descriptors_of_type(
                    member.members, new_position_list, descriptor_type)

                # Remove the member itself if it is a virtual sequence descriptor
                if isinstance(member, descriptor_type):
                    self.remove_virtual_descriptor(*new_position_list)


class VirtualTemplateParser(object):
    def __init__(self, table_group):
        self.table_group = table_group

        lcurly_op = pp.Literal('{')
        rcurly_op = pp.Literal('}')

        integer = pp.Word(pp.nums).setParseAction(lambda tokens: int(tokens[0]))

        element_descriptor = pp.Word('0', pp.nums, exact=6).setParseAction(
            lambda tokens: self.table_group.lookup(tokens[0])
        )
        operator_descriptor = pp.Word('2', pp.nums, exact=6).setParseAction(
            lambda tokens: self.table_group.lookup(tokens[0])
        )

        delayed_replication_descriptor = pp.Combine(
            pp.Word('1', pp.nums, exact=3) + pp.Literal('000')
        ).setParseAction(lambda tokens: self.table_group.lookup(tokens[0]))

        fixed_replication_descriptor = pp.Word('1', pp.nums, exact=6).setParseAction(
            lambda tokens: self.table_group.lookup(tokens[0])
        )

        sequence_descriptor = pp.Word('3', pp.nums, exact=6).setParseAction(
            lambda tokens: self.table_group.lookup(tokens[0])
        )

        virtual_attributes = pp.Forward()

        virtual_attributes << (
            (element_descriptor | operator_descriptor) +
            lcurly_op.suppress() +
            (virtual_attributes | element_descriptor | operator_descriptor) +
            rcurly_op.suppress()
        ).setParseAction(self.virtual_attributes_action)

        stmt = pp.Forward()

        suite = (
            lcurly_op.suppress() +
            pp.OneOrMore(stmt) +
            rcurly_op.suppress()
        )

        virtual_sequence = (
            pp.CaselessLiteral('VirtualSequence').suppress() +
            suite
        ).setParseAction(self.virtual_sequence_action)

        delayed_replication = (
            delayed_replication_descriptor +
            element_descriptor +
            suite
        ).setParseAction(self.delayed_replication_action)

        fixed_replication = (
            fixed_replication_descriptor +
            suite
        ).setParseAction(self.fixed_replication_action)

        virtual_fixed_replication = (
            pp.CaselessLiteral('VirtualFixedReplication').suppress() +
            integer +
            suite
        ).setParseAction(self.virtual_fixed_replication_action)

        stmt << (
            virtual_attributes |
            element_descriptor |
            operator_descriptor |
            sequence_descriptor |
            virtual_sequence |
            delayed_replication |
            fixed_replication |
            virtual_fixed_replication
        )

        template_name = pp.Word(pp.alphas, pp.alphanums + '_').setParseAction(
            lambda tokens: tokens[0]
        )
        self.parser = (
            template_name +
            lcurly_op.suppress() +
            pp.ZeroOrMore(
                stmt
            ) +
            rcurly_op.suppress()
        ).ignore(pp.pythonStyleComment)

    def parse(self, s):
        parsed = self.parser.parseString(s, parseAll=True)
        return VirtualBufrTemplate(999999, parsed[0], parsed[1:])

    def delayed_replication_action(self, tokens):
        delayed_replication = tokens[0]
        delayed_replication.factor = tokens[1]
        delayed_replication.members = tokens[2:]
        return delayed_replication

    def fixed_replication_action(self, tokens):
        fixed_replication = tokens[0]
        fixed_replication.members = tokens[1:]
        return fixed_replication

    def virtual_fixed_replication_action(self, tokens):
        members = tokens[1:]
        return VirtualFixedReplicationDescriptor(
            100000 + len(members) * 1000 + tokens[0],
            members=members
        )

    def virtual_sequence_action(self, tokens):
        return VirtualSequenceDescriptor(300000, 'Virtual Sequence', members=tokens[1:])

    def virtual_attributes_action(self, tokens):
        return VirtualAttributedDescriptor(tokens[0], attributes=tokens[1:])


def _virtualise_members(members, including_fixed_replication):
    """
    Recursively virtualise all of the members and their descendants. The process
    change all Fixed replication and sequence descriptors to their virtual
    counterpart.
    """
    new_members = []
    for member in members:
        if isinstance(member, FixedReplicationDescriptor):
            if including_fixed_replication:
                new_members.append(
                    VirtualFixedReplicationDescriptor.from_fixed_replication_descriptor(
                        member, including_fixed_replication)
                )
            else:
                new_members.append(
                    FixedReplicationDescriptor(
                        member.id,
                        members=_virtualise_members(member.members, including_fixed_replication)
                    )
                )
        elif isinstance(member, SequenceDescriptor):
            new_members.append(
                VirtualSequenceDescriptor.from_sequence_descriptor(member, including_fixed_replication)
            )
        elif isinstance(member, DelayedReplicationDescriptor):
            new_members.append(
                DelayedReplicationDescriptor(
                    member.id,
                    members=_virtualise_members(member.members, including_fixed_replication),
                    factor=member.factor)
            )
        else:
            new_members.append(member)

    return new_members


def dumps_virtual_descriptor(descriptor, indent=''):
    """
    The difference of this function to the function of the same name from
    descriptors are as the follows:
        * A count is prefixed to every element
        * All virtual descriptors are marked by [V]
    Note that the attributes are NOT listed directly under their corresponding
    descriptors. But instead listed in its original positions.
    """
    lines = _dumps_virtual_descriptor(descriptor, indent, 1)
    return '\n'.join(lines)


def _dumps_virtual_descriptor(descriptor, indent, count):
    lines = []

    if isinstance(descriptor, VirtualAttributedDescriptor):
        lines.extend(_dumps_virtual_descriptor(descriptor.descriptor, indent, count))
        for idx, attr in enumerate(descriptor.attributes):
            lines.extend(_dumps_virtual_descriptor(attr, indent + INDENT_CHARS, idx + 1))

    elif isinstance(descriptor, SequenceDescriptor):
        if isinstance(descriptor, VirtualSequenceDescriptor):
            lines.append('{}[{}]{} [V]{}'.format(indent, count, descriptor, descriptor.name))
        elif isinstance(descriptor, VirtualBufrTemplate):
            lines.append('{}{} [V]{}'.format(indent, descriptor, descriptor.name))
        else:
            lines.append('{}[{}]{} {}'.format(indent, count, descriptor, descriptor.name))
        for idx, member in enumerate(descriptor.members):
            lines.extend(_dumps_virtual_descriptor(member, indent + INDENT_CHARS, idx + 1))

    elif isinstance(descriptor, SkippedLocalDescriptor):
        lines.append('{}[{}]{} {} bits'.format(indent, count, descriptor, descriptor.nbits))

    elif isinstance(descriptor, MarkerDescriptor):
        lines.append('{}[{}]{}'.format(indent, count, descriptor))

    elif isinstance(descriptor, ElementDescriptor):
        lines.append('{}[{}]{} {}'.format(indent, count, descriptor, descriptor.name))

    elif isinstance(descriptor, FixedReplicationDescriptor):
        if isinstance(descriptor, VirtualFixedReplicationDescriptor):
            lines.append('{}[{}]{} [V]'.format(indent, count, descriptor))
        else:
            lines.append('{}[{}]{}'.format(indent, count, descriptor))
        for idx, member in enumerate(descriptor.members):
            lines.extend(_dumps_virtual_descriptor(member, indent + INDENT_CHARS, idx + 1))

    elif isinstance(descriptor, DelayedReplicationDescriptor):
        lines.append('{}[{}]{}'.format(indent, count, descriptor))
        lines.extend(_dumps_virtual_descriptor(descriptor.factor, indent + '.' * len(INDENT_CHARS), 0))
        for idx, member in enumerate(descriptor.members):
            lines.extend(_dumps_virtual_descriptor(member, indent + INDENT_CHARS, idx + 1))

    elif isinstance(descriptor, OperatorDescriptor):
        lines.append('{}[{}]{}'.format(indent, count, descriptor))

    elif isinstance(descriptor, AssociatedDescriptor):
        lines.append('{}[{}]{} {} bits'.format(indent, count, descriptor, descriptor.nbits))

    else:
        raise RuntimeError('Unknown descriptor: {}'.format(descriptor))

    return lines
