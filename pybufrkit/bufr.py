"""
pybufrkit.bufr
~~~~~~~~~~~~~~

Classes for representing a BUFR message and its components.
"""
from __future__ import absolute_import
from __future__ import print_function

import json
import os
import logging
import six
from copy import deepcopy
from collections import OrderedDict
from datetime import datetime

from pybufrkit.errors import PyBufrKitError
from pybufrkit.constants import BASE_DIR, NBITS_PER_BYTE, PARAMETER_TYPE_TEMPLATE_DATA
from pybufrkit.tables import get_table_group

log = logging.getLogger(__file__)


class SectionParameter(object):
    """
    This class represents a Parameter of a Bufr Section.
    """

    def __init__(self, name, nbits, data_type, expected, as_property):
        # type: (str, int, str, object, bool) -> None
        self.name = name
        self.nbits = nbits
        self.type = data_type
        # All string type expectation value should be of bytes type not unicode.
        # This is true since current BUFR spec only work with ascii chars.
        if isinstance(expected, six.text_type):
            expected = expected.encode('utf-8')
        self.expected = expected
        self.as_property = as_property
        self.parent = None  # the section that parameter belongs to
        self.value = None


class SectionNamespace(OrderedDict):
    """
    A Section Namespace is an ordered dictionary that store the decoded
    parameters with their names as the keys.
    """

    def __str__(self):
        ret = ['{} = {!r}'.format(k, v.value) for k, v in self.items()]
        return '\n'.join(ret)


# noinspection PyUnresolvedReferences
class BufrSection(object):
    """
    This class represents a Section in a Bufr Message.
    """

    def __init__(self):
        object.__setattr__(self, '_namespace', SectionNamespace())

    def __str__(self):
        return str(self._namespace)

    def __setattr__(self, key, value):
        self._namespace[key] = value

    def __getattr__(self, item):
        return self._namespace[item]

    def __contains__(self, item):
        return item in self._namespace

    def __iter__(self):
        return iter(self._namespace.values())

    def __len__(self):
        return len(self._namespace)

    def set_metadata(self, k, v):
        """
        Set value to a metadata of the given key.

        :param str k: Name of the metadata
        :param object v: Value of the metadata
        """
        # type: (str, object) -> None
        object.__setattr__(self, k, v)

    def get_metadata(self, k):
        """
        Get value for metadata of the given name.

        :param str k: Name of the metadata.
        :return: Value of the metadata
        """
        # type: (str) -> object
        return object.__getattribute__(self, k)

    def add_parameter(self, parameter):
        """
        Add a parameter to the section object.

        :param SectionParameter parameter:
        """
        parameter.parent = self
        self._namespace[parameter.name] = parameter

    def get_parameter_offset(self, parameter_name):
        """
        Get the bit offset from the beginning of the section for parameter of
        the given name.

        :return: The bit offset.
        :rtype: int
        """
        nbits_offset = 0
        for parameter in self:
            if parameter.name == parameter_name:
                return nbits_offset
            else:
                nbits_offset += parameter.nbits
        else:
            raise PyBufrKitError('Parameter "{}" not found'.format(parameter_name))


# An indicator for using the fallback default edition of a section
DEFAULT_SECTION_EDITION = 0


class SectionConfigurer(object):
    """
    This class is responsible for loading the section config JSON files. It also
    initialise and configure a requested Section.
    """

    def __init__(self, definitions_dir=None):
        definitions_dir = definitions_dir or os.path.join(BASE_DIR, 'definitions')
        fnames = [fname for fname in os.listdir(definitions_dir)
                  if fname.startswith('section') and fname.endswith('.json')]

        log.debug("Reading definition json files from {}".format(definitions_dir))

        self.configurations = {}
        for fname in fnames:
            with open(os.path.join(definitions_dir, fname)) as ins:
                index, edition = self.get_section_index_and_edition(fname)
                data = json.load(ins)
                if index not in self.configurations:
                    self.configurations[index] = {}
                config = self.configurations[index]
                edition_key = DEFAULT_SECTION_EDITION if edition is None else edition
                config[edition_key] = data
                # If this config is default, also add it for the default key
                if data.get('default', False) and edition_key != DEFAULT_SECTION_EDITION:
                    config[DEFAULT_SECTION_EDITION] = data

    def configure_section(self, bufr_message, section_index, configuration_transformers=()):
        """
        Initialise and Configure a section for the give section index and
        version.

        :param BufrMessage bufr_message: The Bufr Message object to configure
        :param int section_index: (Zero-based) index of the section
        :param collection configuration_transformers: A collection of configuration
            transformation functions. These functions make it possible to use
            the same set of JSON files while still dynamically providing
            different coder behaviours.
        :return: The configured section or ``None`` if not present
        """

        config = self.get_configuration(bufr_message, section_index)

        for configuration_transformer in configuration_transformers:
            config = configuration_transformer(config)

        section = BufrSection()
        section.set_metadata('index', config['index'])
        section.set_metadata('description', config.get('description', ''))
        section.set_metadata('optional', config.get('optional', False))
        section.set_metadata('end_of_message', config.get('end_of_message', False))

        for parameter in config['parameters']:
            data_type = parameter['type']
            nbits = parameter['nbits']
            if data_type == 'bytes':
                assert nbits % NBITS_PER_BYTE == 0, \
                    'nbits for bytes type must be integer multiple of 8: {}'.format(nbits)
            section_parameter = SectionParameter(
                parameter['name'],
                nbits,
                data_type,
                parameter.get('expected', None),
                parameter.get('as_property', False)
            )
            section.add_parameter(section_parameter)

        # Check whether this section is optional. If it is optional, check whether it exists in the
        # current message.
        is_section_presents = (
            not section.optional or
            getattr(bufr_message, 'is_section{}_presents'.format(section_index)).value
        )

        # If the section exists, add the section for process other wise ignore it
        if is_section_presents:
            bufr_message.add_section(section)
            return section
        else:
            log.info("Section {} is not present".format(section_index))
            return None

    def get_configuration(self, bufr_message, section_index):
        # When the bufr_message is first created, it has no edition information.
        # So the default edition is used.
        if bufr_message.edition is not None:
            section_edition = bufr_message.edition.value or DEFAULT_SECTION_EDITION
        else:
            section_edition = DEFAULT_SECTION_EDITION

        log.info("Configure Section {} of edition {}".format(
            section_index, section_edition if section_edition != DEFAULT_SECTION_EDITION else 'default'))

        section_configs = self.configurations[section_index]

        # Get the default config for the section if specific edition is not found
        return section_configs.get(section_edition, section_configs[DEFAULT_SECTION_EDITION])

    def configure_section_with_values(self, bufr_message, section_index, values):
        """
        Initialise and Configure a section for the give section index and version
        and also populate the value of each section parameter with the given list
        of values. Used by the encoder.

        :param BufrMessage bufr_message: The BUFR message object to configure
        :param int section_index: The zero-based section index
        :param list values: A list of values for the parameters.
        :return: The configured section or ``None`` if the section is not present
        """
        section = self.configure_section(bufr_message, section_index)
        if section is not None:
            assert len(section) == len(values), \
                'Number of Section parameters ({}) not equal to number of values to be encoded ({})'.format(
                    len(section), len(values))
            for idx, parameter in enumerate(section):
                parameter.value = values[idx]

        return section

    @staticmethod
    def get_section_index_and_edition(fname):
        """
        Get Section Index and version from file name of a configuration file.

        :param str fname: The base file name
        :return: The index and edition numbers.
        """
        expr = os.path.splitext(fname)[0][7:]
        if '-' in expr:
            fields = expr.split('-')
            return int(fields[0]), int(fields[1])
        else:
            return int(expr), None

    @staticmethod
    def info_configuration(config):
        """
        This is a configuration transformation function to make the decoder work
        only for the part of message before the template data.

        :param dict config: The config JSON object loaded from a configuration file.
        """
        parameter_types = [parameter['type'] for parameter in config['parameters']]
        if PARAMETER_TYPE_TEMPLATE_DATA in parameter_types:
            new_config = deepcopy(config)
            new_config['end_of_message'] = True
            new_config['parameters'] = config['parameters'][:parameter_types.index(PARAMETER_TYPE_TEMPLATE_DATA)]
            return new_config
        else:
            return config

    @staticmethod
    def ignore_value_expectation(config):
        """
        Remove any expectation value check.

        :param dict config: The config JSON object loaded from a configuration file.
        """
        new_config = deepcopy(config)
        for parameter in new_config['parameters']:
            parameter['expected'] = None
        return new_config


# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class BufrMessage(object):
    """
    This class represents a single BUFR message that is comprised of different
    sections. Note this is different from BufrTemplateData which is only part of
    the overall message and dedicates to data associated to the Template.

    Properties of this class are proxies to actual fields of its sections. They
    are set by the sections when they are processed. The proxy approach allows
    these properties to be referenced in a consistent way no matter where they
    actually come from. This makes sections loosely coupled, i.e. one section
    does not need to know about other sections, and free to change if needed.
    """

    def __init__(self, filename=''):
        self.filename = filename
        self.sections = []
        self.serialized_bytes = None
        self.table_group_key = None
        self._edition = None
        # Default to zero for edition 1 which does not have this parameter
        self._master_table_number = 0
        # Default to zero for edition 1 and 2 which do not have this parameter
        self._originating_subcentre = 0

    def add_section(self, section):
        """
        Add a section to the message

        :param BufrSection section: The Bufr Section to add
        """
        self.sections.append(section)

    def build_template(self, tables_root_dir, normalize=1):
        """
        Build the BufrTemplate object using the list of unexpanded descriptors
        and corresponding table group.

        :param tables_root_dir: The root directory to find BUFR tables
        :param normalize: Whether to use some default table group if the specific
            one is not available.
        :return: A tuple of BufrTemplate and the associated TableGroup
        """
        table_group = get_table_group(
            tables_root_dir=tables_root_dir,
            master_table_number=self.master_table_number.value,
            originating_centre=self.originating_centre.value,
            originating_subcentre=self.originating_subcentre.value,
            master_table_version=self.master_table_version.value,
            local_table_version=self.local_table_version.value,
            normalize=normalize
        )
        self.table_group_key = table_group.key

        return table_group.template_from_ids(*self.unexpanded_descriptors.value), table_group

    def wire(self):
        """
        Wire the flat list of descriptors and values to a full hierarchical
        structure. Also allocate all attributes to their corresponding
        descriptors.
        """
        self.template_data.value.wire()

    def subset(self, subset_indices):
        if max(subset_indices) >= self.n_subsets.value:
            raise PyBufrKitError('maximum subset index out of range')
        if min(subset_indices) < 0:
            raise PyBufrKitError('minimum subset index out of range')

        data = []
        for section in self.sections:
            section_data = []
            for parameter in section:
                if parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                    section_data.append(
                        [v for i, v in enumerate(parameter.value.decoded_values_all_subsets)
                         if i in subset_indices]
                    )
                else:
                    section_data.append(
                        len(subset_indices) if parameter.name == 'n_subsets'
                        else parameter.value
                    )
            data.append(section_data)
        return data

    # Proxy properties follows
    @property
    def length(self):
        return self._length

    @length.setter
    def length(self, new_value):
        self._length = new_value

    @property
    def edition(self):
        return self._edition

    @edition.setter
    def edition(self, new_value):
        self._edition = new_value

    @property
    def master_table_number(self):
        return self._master_table_number

    @master_table_number.setter
    def master_table_number(self, new_value):
        self._master_table_number = new_value

    @property
    def originating_centre(self):
        return self._originating_centre

    @originating_centre.setter
    def originating_centre(self, new_value):
        self._originating_centre = new_value

    @property
    def originating_subcentre(self):
        return self._originating_subcentre

    @originating_subcentre.setter
    def originating_subcentre(self, new_value):
        self._originating_subcentre = new_value

    @property
    def master_table_version(self):
        return self._master_table_version

    @master_table_version.setter
    def master_table_version(self, new_value):
        self._master_table_version = new_value

    @property
    def local_table_version(self):
        return self._local_table_version

    @local_table_version.setter
    def local_table_version(self, new_value):
        self._local_table_version = new_value

    @property
    def timestamp(self):
        # TODO: 1900 correct?
        return datetime(
            self.year.value if self.edition.value >= 4 else self.year.value + 1900,
            self.month.value, self.day.value,
            self.hour.value, self.minute.value, self.second.value
        )

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self, new_value):
        self._year = new_value

    @property
    def month(self):
        return self._month

    @month.setter
    def month(self, new_value):
        self._month = new_value

    @property
    def day(self):
        return self._day

    @day.setter
    def day(self, new_value):
        self._day = new_value

    @property
    def hour(self):
        return self._hour

    @hour.setter
    def hour(self, new_value):
        self._hour = new_value

    @property
    def minute(self):
        return self._minute

    @minute.setter
    def minute(self, new_value):
        self._minute = new_value

    @property
    def second(self):
        return self._second

    @second.setter
    def second(self, new_value):
        self._second = new_value

    @property
    def is_section2_presents(self):
        return self._is_section2_presents

    @is_section2_presents.setter
    def is_section2_presents(self, new_value):
        self._is_section2_presents = new_value

    @property
    def n_subsets(self):
        return self._n_subsets

    @n_subsets.setter
    def n_subsets(self, new_value):
        self._n_subsets = new_value

    @property
    def is_observation(self):
        return self._is_observation

    @is_observation.setter
    def is_observation(self, new_value):
        self._is_observation = new_value

    @property
    def is_compressed(self):
        return self._is_compressed

    @is_compressed.setter
    def is_compressed(self, new_value):
        self._is_compressed = new_value

    @property
    def unexpanded_descriptors(self):
        return self._unexpanded_descriptors

    @unexpanded_descriptors.setter
    def unexpanded_descriptors(self, new_value):
        self._unexpanded_descriptors = new_value

    @property
    def template_data(self):
        return self._template_data

    @template_data.setter
    def template_data(self, new_value):
        self._template_data = new_value
