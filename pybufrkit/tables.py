"""
pybufrkit.tables
~~~~~~~~~~~~~~~~

The Table Cache makes sure tables of the same version only get loaded from
disk once. When the get_table_group method is called, it returned a
group of table either from the cache or loaded from disk if they are not
available in the cache yet (and save them to the cache for future use).

* A Table Group contains a set of tables, e.g. A, B, C, D, that belong to the
  same table group key.

* A Table instance, e.g. B, D, maintains a cache of its descriptors so that
  only a single instance is created for an unique descriptor.

* The Pseudo Replication Descriptor table is created to make the API for all
  tables look alike.

* TableCache --creates--> TableGroup --lookup--> Descriptors/Template

Template are then processed by Coder in conjunction with a bit operator to
create a BufrMessage object.
"""
from __future__ import print_function
from __future__ import absolute_import
import os
import json
import logging
import functools
from numbers import Integral
from collections import namedtuple

# noinspection PyUnresolvedReferences
from six.moves import range

from pybufrkit.constants import DEFAULT_TABLES_DIR
from pybufrkit.descriptors import (ElementDescriptor,
                                   FixedReplicationDescriptor, DelayedReplicationDescriptor,
                                   OperatorDescriptor, SequenceDescriptor, BufrTemplate,
                                   UndefinedElementDescriptor, UndefinedSequenceDescriptor)
from pybufrkit.utils import generate_quiet

__all__ = ['TableGroupKey', 'TableGroupCacheManager']

log = logging.getLogger(__file__)

TableGroupKey = namedtuple('TableGroupKey', ['tables_root_dir',
                                             'wmo_tables_sn',
                                             'local_tables_sn'])

# The maximum number of table groups to be cached
MAXIMUM_NUMBER_OF_CACHED_TABLE_GROUPS = 50

# TODO: These defaults should be externalized
DEFAULT_MASTER_TABLE_NUMBER = 0
DEFAULT_ORIGINATING_CENTRE = 0
DEFAULT_ORIGINATING_SUBCENTRE = 0
DEFAULT_MASTER_TABLE_VERSION = 33
DEFAULT_LOCAL_TABLE_VERSION = 0


def get_tables_sn(master_table_number,
                  originating_centre,
                  originating_subcentre,
                  master_table_version,
                  local_table_version):
    wmo_tables_sn = (str(master_table_number), '0_0', str(master_table_version))
    if local_table_version != 0:
        local_tables_sn = (str(master_table_number),
                           '{}_{}'.format(originating_centre, originating_subcentre),
                           str(local_table_version))
    else:
        local_tables_sn = None

    return wmo_tables_sn, local_tables_sn


def normalize_tables_sn(tables_root_dir,
                        master_table_number,
                        originating_centre,
                        originating_subcentre,
                        master_table_version,
                        local_table_version):
    """
    This function is to take the given table directory and sn and check whether
    the tables (using Table B as the measure) are actually available. If not, it
    then attempts to replace the given SN with the default sn. If the tables are
    still not found, then also replace directory with the default value.

    :param originating_centre:
    :param originating_subcentre:
    :param master_table_version:
    :param local_table_version:
    :param master_table_number:
    :param tables_root_dir:
    :return: The directory and SN for which tables can actually be found
    :rtype: (str, str)
    """

    master_table_number_string = str(master_table_number)
    master_table_version_string = str(master_table_version)
    local_table_version_string = str(local_table_version)

    # Ensure the master table number exists
    if not os.path.isdir(os.path.join(tables_root_dir, master_table_number_string)):
        log.warning('Fallback to default master table number: {} ({} not found)'.format(
            DEFAULT_MASTER_TABLE_NUMBER, master_table_number_string
        ))
        master_table_number_string = str(DEFAULT_MASTER_TABLE_NUMBER)

    # Find out WMO tables to base from
    centres = '0_0'
    if os.path.isdir(os.path.join(tables_root_dir,
                                  master_table_number_string,
                                  centres,
                                  master_table_version_string
                                  )):
        wmo_tables_sn = (master_table_number_string, centres, master_table_version_string)
    else:
        log.warning('Fallback to default master table version {} ({} not found)'.format(
            DEFAULT_MASTER_TABLE_VERSION, master_table_version_string
        ))
        wmo_tables_sn = (master_table_number_string, centres, str(DEFAULT_MASTER_TABLE_VERSION))

    # find out local tables if needed
    if local_table_version != 0:  # local table in use
        centres_candidates = [
            '{}_{}'.format(originating_centre, originating_subcentre),
            '{}_{}'.format(originating_centre, DEFAULT_ORIGINATING_SUBCENTRE),
        ]
        for idx, centres in enumerate(centres_candidates):
            if os.path.isdir(os.path.join(tables_root_dir,
                                          master_table_number_string,
                                          centres,
                                          local_table_version_string)):
                local_tables_sn = (master_table_number_string, centres, local_table_version_string)
                if idx != 0:
                    log.warning('Fallback to default local sub-centre {} ({} not found)'.format(
                        DEFAULT_ORIGINATING_SUBCENTRE, originating_subcentre
                    ))
                break
        else:
            log.warning('Cannot find sub-centre {} nor valid default. Local table not in use.'.format(
                originating_subcentre
            ))
            local_tables_sn = None

    else:  # no local table is used
        local_tables_sn = None

    return wmo_tables_sn, local_tables_sn


class BaseTable(object):
    def __init__(self, table_group_key, extra_entries=()):
        self.table_group_key = table_group_key
        self.extra_entries = extra_entries

    def __eq__(self, other):
        return type(self) is type(other) and self.table_group_key == other.table_group_key

    def __str__(self):
        return '{}: {} - {}, {}'.format(
            self.__class__.__name__,
            self.table_group_key.tables_root_dir,
            self.table_group_key.wmo_tables_sn,
            self.table_group_key.local_tables_sn
        )

    @property
    def type(self):
        return self.__class__.__name__[-1]

    @property
    def tables_dir_wmo(self):
        return os.path.join(os.path.join(self.table_group_key.tables_root_dir, *self.table_group_key.wmo_tables_sn))

    @property
    def tables_dir_local(self):
        return (
            os.path.join(os.path.join(self.table_group_key.tables_root_dir, *self.table_group_key.local_tables_sn))
            if self.table_group_key.local_tables_sn else None
        )

    def load_json_files(self, fname):
        contents = []
        for tables_dir in (self.tables_dir_wmo, self.tables_dir_local):
            if tables_dir is None:
                continue
            with open(os.path.join(tables_dir, fname)) as ins:
                contents.append(json.load(ins))

        if self.extra_entries:
            contents.append(self.extra_entries)

        return contents


class TableA(BaseTable):
    pass


class TableB(BaseTable):
    def __init__(self, *args, **kwargs):
        super(TableB, self).__init__(*args, **kwargs)

        self.code_and_flag = {}

        self.descriptors = {}

        # Load WMO and Local tables
        for data in self.load_json_files('TableB.json'):
            for id_string, fields in data.items():
                id_ = int(id_string)
                self.descriptors[id_] = ElementDescriptor(id_, *fields)

    def load_code_and_flag(self):
        if self.code_and_flag:
            return

        for data in self.load_json_files('code_and_flag.json'):
            self.code_and_flag.update(data)

    def lookup(self, id_):
        # TODO: allow unknown ID to be instantiated as skipped descriptor?
        if not isinstance(id_, Integral):
            id_ = int(id_)

        try:
            descriptor = self.descriptors[id_]
        except KeyError:
            descriptor = UndefinedElementDescriptor(id_)

        return descriptor

    def code_and_flag_for_descriptor(self, descriptor):
        return self.code_and_flag.get(str(descriptor), None)


class TableC(BaseTable):
    def __init__(self, *args, **kwargs):
        super(TableC, self).__init__(*args, **kwargs)
        self._cache = {}

    def lookup(self, id_):
        if not isinstance(id_, Integral):
            id_ = int(id_)
        if id_ not in self._cache:
            self._cache[id_] = OperatorDescriptor(id_)
        return self._cache[id_]


class TableR(BaseTable):
    """
    This is a Pseudo-table for hosting replication descriptors. NOTE that
    replication descriptors are NOT cached as the same replication descriptor
    can be applied to different group of descriptors. However, since Sequence
    Descriptors are cached, they will also cache the replication descriptors
    associated with them.
    """

    def lookup(self, id_):
        if not isinstance(id_, Integral):
            id_ = int(id_)
        if id_ % 1000 == 0:
            return DelayedReplicationDescriptor(id_)
        else:
            return FixedReplicationDescriptor(id_)


def _descriptors_from_ids(b, c, r, d, ids):
    g = (id_ if isinstance(id_, Integral) else int(id_) for id_ in ids)
    return _descriptors_from_ids_iter(b, c, r, d, functools.partial(next, g))


def _descriptors_from_ids_iter(b, c, r, d, next_id):
    descriptors = []
    while True:
        try:
            id_ = next_id()
            if not isinstance(id_, Integral):
                id_ = int(id_)
        except StopIteration:
            break
        if id_ >= 300000:
            descriptors.append(d.lookup(id_))

        elif id_ >= 200000:
            descriptors.append(c.lookup(id_))

        elif id_ >= 100000:
            descriptor = r.lookup(id_)
            if isinstance(descriptor, DelayedReplicationDescriptor):
                descriptor.factor = b.lookup(next_id())

            g = generate_quiet(range(descriptor.n_items), next_id)
            # TODO: check whether the actual number of members equals to n_items
            descriptor.members = _descriptors_from_ids_iter(b, c, r, d, functools.partial(next, g))
            descriptors.append(descriptor)

        else:  # element descriptor
            # TODO: if this descriptor is preceded by a 206XXX, it should turn into skipped local
            descriptors.append(b.lookup(id_))

    return descriptors


class TableD(BaseTable):
    def __init__(self, b, c, r, *args, **kwargs):
        super(TableD, self).__init__(*args, **kwargs)
        self.descriptors = {}

        # Load WMO and Local tables
        for data in self.load_json_files('TableD.json'):

            # TODO: allow undefined sequence descriptors as long as they are not actually used in decoding?
            # This is to allow incomplete Table D files. May not be necessary (no need to be too tolerant)
            sorted_ids = sorted(data.keys())

            # First create all the sequence descriptors without members
            # so that they can reference each other without failing
            for id_string in sorted_ids:
                id_ = int(id_string)
                name = data[id_string][0]
                self.descriptors[id_] = SequenceDescriptor(id_, name, None)

            # Now populate the actual members
            for id_string in sorted_ids:
                id_ = int(id_string)
                member_ids = data[id_string][1]
                members = _descriptors_from_ids(b, c, r, self, member_ids)
                self.descriptors[id_].members = members

    def lookup(self, id_):
        if not isinstance(id_, Integral):
            id_ = int(id_)
        try:
            descriptor = self.descriptors[id_]
        except KeyError:
            descriptor = UndefinedSequenceDescriptor(id_)

        return descriptor


_BufrTableGroup = namedtuple('BufrTableGroup', ['A', 'B', 'C', 'D', 'R'])


# TODO: allow non-exist B element during table loading by creating a
# BadDescriptor and errors out if it is actually used in later decoding
class BufrTableGroup(_BufrTableGroup):
    """
    A group of tables (A, B, C, D) that belong to the same tables directory and
    SN. It is responsible for getting or creating Descriptors via lookup.

    Itself is created by the singleton TableCache.
    """

    def __eq__(self, other):
        return (
                isinstance(other, BufrTableGroup) and
                self.A == other.A and
                self.B == other.B and
                self.C == other.C and
                self.D == other.D and
                self.R == other.R
        )

    def __str__(self):
        return '<{}: {!r} - {}, {}>'.format(
            self.__class__.__name__,
            self.A.tables_root_dir,
            self.A.wmo_tables_sn,
            self.A.local_tables_sn,
        )

    def __repr__(self):
        return str(self)

    @property
    def key(self):
        return self.A.table_group_key

    def lookup(self, id_):
        """
        This method is different from `descriptors_from_ids` in that it does not
        try to load any members or replication factors for a replication
        descriptor. It just performs simple dictionary lookup.
        """
        id_ = id_ if isinstance(id_, Integral) else int(id_)

        if id_ >= 300000:
            descriptor = self.D.lookup(id_)
        elif id_ >= 200000:
            descriptor = self.C.lookup(id_)
        elif id_ >= 100000:
            descriptor = self.R.lookup(id_)
        else:
            descriptor = self.B.lookup(id_)

        return descriptor

    def descriptors_from_ids(self, *ids):
        """
        Load descriptor for the given list of IDs. If a replication descriptor
        is found in the list of IDs, the code tries to load all of its members
        and replication factor (if it is delayed replication) from the the given
        list as well.

        :param [int] ids: A list of raw IDs
        :return: A list of descriptors with the given IDs
        :rtype: list
        """
        return _descriptors_from_ids(self.B, self.C, self.R, self.D, ids)

    def template_from_ids(self, *ids):
        """
        Build BUFR Template from a list of IDs
        """
        members = self.descriptors_from_ids(*ids)
        if TableGroupCacheManager.has_extra_entries():
            members = _fix_ncep_descriptors(members)
        bufr_template = BufrTemplate(members=members)
        return bufr_template


def _fix_ncep_descriptors(descriptors):
    """
    Fix ill-defined sequence descriptor, e.g. NCEP 360001, which is consisted of
    101000, 031002. Note that the actual descriptor to replicated is not part of
    the sequence descriptor! This is pure evil!!
    """
    from copy import deepcopy
    ret = []
    while len(descriptors) > 0:
        descriptor = deepcopy(descriptors.pop(0))
        if isinstance(descriptor, SequenceDescriptor):
            if (len(descriptor.members) == 1 and
                    isinstance(descriptor.members[0], (FixedReplicationDescriptor, DelayedReplicationDescriptor)) and
                    len(descriptor.members[0].members) == 0):
                descriptors.insert(0, descriptor.members[0])
            else:
                descriptor.members = _fix_ncep_descriptors(descriptor.members)
                ret.append(descriptor)
        elif isinstance(descriptor, (FixedReplicationDescriptor, DelayedReplicationDescriptor)):
            if len(descriptor.members) == 0:
                assert descriptor.n_items == 1, 'Fix for replication descriptor expects 1 member, got {}'.format(
                    len(descriptor.members))
                descriptor.members = [descriptors.pop(0)]
            descriptor.members = _fix_ncep_descriptors(descriptor.members)
            ret.append(descriptor)
        else:
            ret.append(descriptor)
    return ret


class TableGroupCache(object):
    """
    The cache keep a single copy for a table group with unique directory and sn. When
    it is requested multiple times, it only needs to be loaded from disk once.

    Note only Table B and D are really cached, Table A and C are singletons.
    """

    def __init__(self):
        # TODO: set maximum number of table groups to be cached
        self._groups = {}
        self.extra_b_entries = {}
        self.extra_d_entries = {}

    def get(self, table_group_key):
        if table_group_key not in self._groups:
            # Honor the setting of maximum number of cached table groups
            if len(self._groups) >= MAXIMUM_NUMBER_OF_CACHED_TABLE_GROUPS:
                for _ in range(len(self._groups) + 1 - MAXIMUM_NUMBER_OF_CACHED_TABLE_GROUPS):
                    self._groups.popitem()

            a = TableA(table_group_key)
            b = TableB(table_group_key, self.extra_b_entries)
            c = TableC(table_group_key)
            r = TableR(table_group_key)
            d = TableD(b, c, r, table_group_key, self.extra_d_entries)
            self._groups[table_group_key] = BufrTableGroup(a, b, c, d, r)

        return self._groups[table_group_key]

    def has_extra_entries(self):
        return self.extra_b_entries or self.extra_d_entries

    def invalidate(self):
        self._groups.clear()

    def add_extra_entries(self, b_entries, d_entries):
        self.extra_b_entries.update(b_entries)
        self.extra_d_entries.update(d_entries)


class TableGroupCacheManager(object):
    _TABLE_GROUP_CACHE = TableGroupCache()

    @classmethod
    def has_extra_entries(cls):
        return cls._TABLE_GROUP_CACHE.has_extra_entries()

    @classmethod
    def invalidate(cls):
        cls._TABLE_GROUP_CACHE.invalidate()

    @classmethod
    def add_extra_entries(cls, b_entries, d_entries):
        cls._TABLE_GROUP_CACHE.add_extra_entries(b_entries, d_entries)

    @classmethod
    def get_table_group_by_key(cls, table_group_key):
        """
        Retrieve a Table Group with the given key.

        :param TableGroupKey table_group_key: The key for a table group.
        :rtype: BufrTableGroup
        """
        log.debug('Reading table group of key: {}'.format(table_group_key))
        return cls._TABLE_GROUP_CACHE.get(table_group_key)

    @classmethod
    def get_table_group(cls,
                        tables_root_dir=None,
                        master_table_number=None,
                        originating_centre=None,
                        originating_subcentre=None,
                        master_table_version=None,
                        local_table_version=None,
                        normalize=True):
        """
        Retrieve a Table Group using given information that can be converted
        to a Table Group Key.

        :param master_table_number:
        :param originating_centre:
        :param originating_subcentre:
        :param master_table_version:
        :param local_table_version:
        :param str tables_root_dir: Root directory to read the BUFR tables
        :param int|bool normalize: Whether the program tries to fix non-exist tables SN by
                          using default values. This is generally useful for
                          decoding. But could be misleading when encoding.
        :rtype: BufrTableGroup
        """

        tables_root_dir = tables_root_dir or DEFAULT_TABLES_DIR

        if normalize:
            wmo_tables_sn, local_tables_sn = normalize_tables_sn(
                tables_root_dir,
                master_table_number or DEFAULT_MASTER_TABLE_NUMBER,
                originating_centre or DEFAULT_ORIGINATING_CENTRE,
                originating_subcentre or DEFAULT_ORIGINATING_SUBCENTRE,
                master_table_version or DEFAULT_MASTER_TABLE_VERSION,
                local_table_version or DEFAULT_LOCAL_TABLE_VERSION
            )
        else:
            wmo_tables_sn, local_tables_sn = get_tables_sn(
                master_table_number,
                originating_centre,
                originating_subcentre,
                master_table_version,
                local_table_version
            )

        table_group_key = TableGroupKey(tables_root_dir, wmo_tables_sn, local_tables_sn)

        # TODO: catch error on file reading?
        return TableGroupCacheManager.get_table_group_by_key(table_group_key)
