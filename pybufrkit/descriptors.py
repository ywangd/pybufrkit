"""
The Descriptors should always be instantiated by Tables. Because the Tables
provide caching and other wiring work. NEVER instantiated the Descriptors
directly !!!!
"""
from .utils import BpclError, INDENT_CHARS


class DescriptorBase(object):
    def __init__(self, id_):
        self.id = id_

    def __str__(self):
        return '{:06d}'.format(self.id)

    def __repr__(self):
        return '<{}>'.format(self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    @property
    def F(self):
        return self.id // 100000

    @property
    def X(self):
        return self.id // 1000 % 100

    @property
    def Y(self):
        return self.id % 1000

    def dumps(self):
        return dumps_descriptor(self)

    def _serialize(self, indent, count):
        return ['{}{}  # [{}]'.format(indent, self, count)]


class AssociatedDescriptor(DescriptorBase):
    """
    Associated field for element descriptor
    """

    def __init__(self, id_, nbits):
        super(AssociatedDescriptor, self).__init__(id_)
        self.nbits = nbits
        # Dummy unit so it does not fail on unit checking in other functions
        self.unit = 'ASSOCIATED'

    def __str__(self):
        return 'A{:05d}'.format(self.id)


class SkippedLocalDescriptor(DescriptorBase):
    """
    For an example, the skipped local descriptor. This descriptor can actually
    exist in an template as long as it is preceded by a 206YYY.
    """

    def __init__(self, id_, nbits):
        # TODO: If a local descriptor does not exist in tables, it will be
        # created as an undefined descriptor. So how it should be converted
        # later to SkippedLocalDescriptor if it has a preceding 206YYY? During
        # template lint?
        super(SkippedLocalDescriptor, self).__init__(id_)
        self.nbits = nbits
        # Dummy unit so it does not fail on unit checking in other functions
        self.unit = 'SKIPPED'

    def __str__(self):
        return 'S{:05d}'.format(self.id)


class ElementDescriptor(DescriptorBase):
    def __init__(self, id_, name, unit, scale, refval, nbits,
                 crex_unit, crex_scale, crex_nchars):
        super(ElementDescriptor, self).__init__(id_)
        self.name, self.unit, self.scale, self.refval, self.nbits = (
            name, unit, scale, refval, nbits
        )
        self.crex_unit, self.crex_scale, self.crex_nchars = crex_unit, crex_scale, crex_nchars

    def as_list(self):
        return [self.id, self.name, self.unit, self.scale, self.refval, self.nbits]

    def as_dict(self):
        return {self.id: (self.name, self.unit, self.scale, self.refval, self.nbits)}

    def _serialize(self, indent, count):
        return ['{}{}  # [{}]{}'.format(indent, self, count, self.name)]


marker_descriptor_prefix = {
    223255: 'T',  # substituted value
    224255: 'F',  # First order stats
    225255: 'D',  # Difference stats
    232255: 'R',  # replaced/retained value
}


class MarkerDescriptor(ElementDescriptor):
    """
    A marker descriptor is useful in the case when marker operator descriptors
    are used to signify a statistical value of an element descriptor. For an
    example, 224255 and 225255.
    """

    def __str__(self):
        return '{}{:05d}'.format(
            marker_descriptor_prefix.get(self.marker_id, 'M'),
            self.id
        )

    @staticmethod
    def from_element_descriptor(ed, marker_id,
                                scale=None, refval=None, nbits=None):
        """
        Create from a given element descriptor with the option to override its
        scale, refval and nbits.

        :param ElementDescriptor ed: The element descriptor
        :param marker_id: The marker operator ID
        :param scale: New override value for scale.
        :param refval:
        :param nbits:
        """
        md = MarkerDescriptor(
            ed.id, ed.name, ed.unit,
            ed.scale if scale is None else scale,
            ed.refval if refval is None else refval,
            ed.nbits if nbits is None else nbits,
            ed.crex_unit, ed.crex_scale, ed.crex_nchars
        )
        md.marker_id = marker_id
        return md


class ReplicationDescriptorBase(DescriptorBase):
    """
    The replication factor member stores only the replication factor descriptor
    NOT the actual value. So it is OK as it should be reusable for the same
    sequence descriptor. That is to say, when a Sequence Descriptor, e.g.
    309052, is reused, the Replication Descriptor inside it should always have
    the same replication factor descriptor. Although these replication factor
    descriptor can have different values in different reuses of 309052, it does
    not matter as it does not store the actual values.

    When the replication descriptor is reused as naked descriptor, i.e. not part
    of a Sequence Descriptor but directly under a Template, the associated
    replication factor descriptor could be different. But since the replication
    descriptor is NOT cached when used as naked. Every time a new Replication
    Descriptor is spawn thus there is no risk on the associated replication
    factor descriptor gets mixed up.

    :param [DescriptorBase] members: The group of descriptors to be replicated
    """

    def __init__(self, id_, members=None):
        super(ReplicationDescriptorBase, self).__init__(id_)
        self.members = members

    @property
    def n_items(self):
        return (self.id // 1000) % 100

    @property
    def n_members(self):
        """
        Due to the hierarchical structure of the BUFR Template, The number of
        members is not always equal to number of items. For an example, the
        delayed replication factor counts towards number of items to be repeated
        for its outer replication (if nested). However, it will never be counted
        towards number of members. Other potential difference comes from Virtual
        descriptors, where virtual sequences and fixed replications are
        inserted/removed without fixing the enclosing replication descriptors.
        So in summary, the number of members is a more accurate count of the
        members to be replicated by the replication descriptor.
        """
        return len(self.members)


class FixedReplicationDescriptor(ReplicationDescriptorBase):
    def __init__(self, id_, members=None):
        super(FixedReplicationDescriptor, self).__init__(id_, members)

    @property
    def n_repeats(self):
        return self.id % 1000

    def _serialize(self, indent, count):
        lines = ['{}{} {}  # [{}]'.format(indent, self, '{', count)]
        for idx, member in enumerate(self.members):
            lines.extend(member._serialize(indent + INDENT_CHARS, idx + 1))
        lines.append('{}{}'.format(indent, '}'))
        return lines


class DelayedReplicationDescriptor(ReplicationDescriptorBase):
    def __init__(self, id_, members=None, factor=None):
        super(DelayedReplicationDescriptor, self).__init__(id_, members)
        self.factor = factor

    @property
    def n_repeats(self):
        raise BpclError('Cannot access n_repeats for Delayed Replication')

    def _serialize(self, indent, count):
        lines = ['{}{} {} {}  # [{}] [0]'.format(indent, self, self.factor, '{', count)]
        for idx, member in enumerate(self.members):
            lines.extend(member._serialize(indent + INDENT_CHARS, idx + 1))
        lines.append('{}{}'.format(indent, '}'))
        return lines


class OperatorDescriptor(DescriptorBase):
    def __init__(self, id_):
        super(OperatorDescriptor, self).__init__(id_)

    @property
    def operator_code(self):
        return self.id // 1000

    @property
    def operand_value(self):
        return self.id % 1000


# noinspection PyAttributeOutsideInit
class SequenceDescriptor(DescriptorBase):
    def __init__(self, id_, name, members=None):
        super(SequenceDescriptor, self).__init__(id_)
        self.members = members
        self.name = name

    def __iter__(self):
        return iter(self.members)

    def __getitem__(self, item):
        return self.members.__getitem__(item)

    def _serialize(self, indent, count):
        lines = ['{}{}  # [{}]{}'.format(indent, self, count, self.name)]
        for idx, member in enumerate(self.members):
            lines.extend(member._serialize(indent + INDENT_CHARS, idx + 1))
        lines.append('{}{}'.format(indent, '}'))
        return lines


class BufrTemplate(SequenceDescriptor):
    def __init__(self, id_=999999, name='', members=None):
        super(BufrTemplate, self).__init__(id_, name, members)

    def __str__(self):
        return 'TEMPLATE'


class UndefinedDescriptor(DescriptorBase):
    """
    Undefined Descriptors are only useful when loading incompletely defined
    table files. For an example, an element descriptor is used in one of the
    sequence definition but the element descriptor is not defined. In this case,
    a Undefined descriptor is created in place of the actual element descriptor
    to allow tables to be load. As long as this Undefined descriptor is not used
    in the actual decoding. It is harmless to stay in the tables.
    """

    def __init__(self, id_):
        super(UndefinedDescriptor, self).__init__(id_)

    def __str__(self):
        return 'UNDEFINED ({:06d})'.format(self.id)


class UndefinedElementDescriptor(UndefinedDescriptor):
    pass


class UndefinedSequenceDescriptor(DescriptorBase):
    pass


def flat_member_ids(descriptor):
    """
    Return a flat list of expanded numeric IDs for the given descriptor.

    :param descriptor:
    :return: [int]
    """
    ret = []
    for member in descriptor.members:
        if isinstance(member, SequenceDescriptor):
            ret.extend(flat_member_ids(member))
        elif isinstance(member, FixedReplicationDescriptor):
            ret.append(member.id)
            ret.extend(flat_member_ids(member))
        elif isinstance(member, DelayedReplicationDescriptor):
            ret.append(member.id)
            ret.append(member.factor.id)
            ret.extend(flat_member_ids(member))
        else:
            ret.append(member.id)

    return ret


def dumps_descriptor(descriptor, indent=''):
    lines = _dumps_descriptor(descriptor, indent)
    return '\n'.join(lines)


def _dumps_descriptor(descriptor, indent):
    lines = []

    if isinstance(descriptor, SequenceDescriptor):
        lines.append('{}{} {}'.format(indent, descriptor, descriptor.name))
        for member in descriptor.members:
            lines.extend(_dumps_descriptor(member, indent + INDENT_CHARS))

    elif isinstance(descriptor, SkippedLocalDescriptor):
        lines.append('{}{} {} bits'.format(indent, descriptor, descriptor.nbits))

    elif isinstance(descriptor, MarkerDescriptor):
        lines.append('{}{}'.format(indent, descriptor))

    elif isinstance(descriptor, ElementDescriptor):
        lines.append('{}{} {}'.format(indent, descriptor, descriptor.name))

    elif isinstance(descriptor, FixedReplicationDescriptor):
        lines.append('{}{}'.format(indent, descriptor))
        for member in descriptor.members:
            lines.extend(_dumps_descriptor(member, indent + INDENT_CHARS))

    elif isinstance(descriptor, DelayedReplicationDescriptor):
        lines.append('{}{}'.format(indent, descriptor))
        lines.extend(_dumps_descriptor(descriptor.factor, indent + '.' * len(INDENT_CHARS)))
        for member in descriptor.members:
            lines.extend(_dumps_descriptor(member, indent + INDENT_CHARS))

    elif isinstance(descriptor, OperatorDescriptor):
        lines.append('{}{}'.format(indent, descriptor))

    elif isinstance(descriptor, AssociatedDescriptor):
        lines.append('{}{} {} bits'.format(indent, descriptor, descriptor.nbits))

    else:
        raise BpclError('Unknown descriptor: {}'.format(descriptor))

    return lines
