"""
pybufrkit.descriptors
~~~~~~~~~~~~~~~~~~~~~

The Descriptors should always be instantiated by Tables. Because the Tables
provide caching and other wiring work. Do NOT instantiated the Descriptors
directly!!

This module contains many Descriptor classes, covering not only the canonical
descriptor types of the BUFR spec, but also Conceptual Descriptors that help the
processing. For an example, an AssociatedDescriptor class is needed to represent
associated values signified by operator descriptor 204YYY.
"""
from __future__ import absolute_import
from __future__ import print_function

from pybufrkit.errors import PyBufrKitError


class Descriptor(object):
    """
    This class is the base class of all BUFR descriptors. It provides common
    machinery for Descriptors.

    :param int id_: The descriptor ID.
    """
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
        """
        The F value of the descriptor.
        """
        return self.id // 100000

    @property
    def X(self):
        """
        The X value of the descriptor.
        """
        return self.id // 1000 % 100

    @property
    def Y(self):
        """
        THe Y value of the descriptor.
        """
        return self.id % 1000


class AssociatedDescriptor(Descriptor):
    """
    Associated field for element descriptor

    :param int nbits: Number of bits used by this descriptor.
    """

    def __init__(self, id_, nbits):
        super(AssociatedDescriptor, self).__init__(id_)
        self.nbits = nbits
        # Dummy unit so it does not fail on unit checking in other functions
        self.unit = 'ASSOCIATED'

    def __str__(self):
        return 'A{:05d}'.format(self.id)


class SkippedLocalDescriptor(Descriptor):
    """
    The skipped local descriptor is a placeholder for any descriptors followed by
    operator descriptor 206YYY.
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


class ElementDescriptor(Descriptor):
    """
    Element Descriptor 0XXYYY

    :param int id_: The descriptor ID
    :param str name: Name of the descriptor
    :param str unit: Units of the descriptor
    :param int scale: Scale factor of the descriptor value
    :param int refval: Reference value of the descriptor value
    :param int nbits: The number of bits used by the descriptor
    :param str crex_unit: Units of the descriptor for CREX spec
    :param int crex_scale: Scale factor of the descriptor value for CREX Spec
    :param int crex_nchars: Number of characters used by the descriptor for CREX Spec
    """
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
        :param int marker_id: The marker operator ID
        :param int scale: Overridden value for scale.
        :param int refval: Overridden value for reference.
        :param int nbits: Overridden value for number of bits.
        :rtype: MarkerDescriptor
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


class ReplicationDescriptor(Descriptor):
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

    :param [Descriptor] members: The group of descriptors to be replicated
    """

    def __init__(self, id_, members=None):
        super(ReplicationDescriptor, self).__init__(id_)
        self.members = members

    @property
    def n_items(self):
        """
        Number of descriptors to be replicated. This value is decoded from the
        ID of the descriptor.
        """
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


class FixedReplicationDescriptor(ReplicationDescriptor):
    """
    Fixed replication Descriptor 1XXYYY
    """
    def __init__(self, id_, members=None):
        super(FixedReplicationDescriptor, self).__init__(id_, members)

    @property
    def n_repeats(self):
        """
        Number of times to perform the replication. This value is decoded
        directly from the descriptor ID.
        """
        return self.id % 1000


class DelayedReplicationDescriptor(ReplicationDescriptor):
    """
    Delayed replication Descriptor 1XX000
    """
    def __init__(self, id_, members=None, factor=None):
        super(DelayedReplicationDescriptor, self).__init__(id_, members)
        self.factor = factor

    @property
    def n_repeats(self):
        raise PyBufrKitError('Cannot access n_repeats for Delayed Replication')


class OperatorDescriptor(Descriptor):
    """
    Operator Descriptor 2XXYYY
    """
    def __init__(self, id_):
        super(OperatorDescriptor, self).__init__(id_)

    @property
    def operator_code(self):
        return self.id // 1000

    @property
    def operand_value(self):
        return self.id % 1000


# noinspection PyAttributeOutsideInit
class SequenceDescriptor(Descriptor):
    """
    Sequence Descriptor 3XXYYY
    """
    def __init__(self, id_, name, members=None):
        super(SequenceDescriptor, self).__init__(id_)
        self.members = members
        self.name = name

    def __iter__(self):
        return iter(self.members)

    def __getitem__(self, item):
        return self.members.__getitem__(item)


class BufrTemplate(SequenceDescriptor):
    """
    This class represents a BUFR Template. A Template is composed of one or more
    BUFR Descriptors. It is used in a BUFR message to describe the data section.
    """
    def __init__(self, id_=999999, name='', members=None):
        super(BufrTemplate, self).__init__(id_, name, members)

    def __str__(self):
        return 'BufrTemplate'

    @property
    def original_descriptor_ids(self):
        """
        Get the list of descriptor IDs that can be used to instantiate the Template.

        :rtype [int]
        """
        ret = []
        members = list(self.members)
        while members:
            member = members.pop(0)
            ret.append(member.id)
            if isinstance(member, ReplicationDescriptor):
                if isinstance(member, DelayedReplicationDescriptor):
                    ret.append(member.factor.id)
                members = member.members + members

        return ret


class UndefinedDescriptor(Descriptor):
    """
    Undefined Descriptors are only useful when loading BUFR tables that are NOT
    completely defined. For an example, an element descriptor is used by one of
    the sequence descriptor but the element descriptor itself is not defined in
    Table B. In this case, an Undefined descriptor is created in place of the
    actual element descriptor to allow tables to be loaded normally. As long as
    the Undefined descriptor is not used in the actual decoding (the Template of
    a BUFR message may not contain the descriptor at all), it is harmless to
    stay in the loaded Table Group.

    Ideally this is not necessary if all tables are well defined. However, in
    practice, this is needed so some not-well-defined local tables can be used.
    """

    def __init__(self, id_):
        super(UndefinedDescriptor, self).__init__(id_)

    def __str__(self):
        return 'UNDEFINED ({:06d})'.format(self.id)


class UndefinedElementDescriptor(UndefinedDescriptor):
    pass


class UndefinedSequenceDescriptor(Descriptor):
    pass


def flat_member_ids(descriptor):
    """
    Return a flat list of expanded numeric IDs for the given descriptor. The
    list is generated by recursively flatten all its child members.

    :param Descriptor descriptor: A BUFR descriptor
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
