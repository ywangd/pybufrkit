"""
pybufrkit.renderer
~~~~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import abc
from collections import OrderedDict

# noinspection PyUnresolvedReferences
from six.moves import range, zip

from pybufrkit.constants import INDENT_CHARS, PARAMETER_TYPE_TEMPLATE_DATA
from pybufrkit.errors import PyBufrKitError, UnknownDescriptor
from pybufrkit.utils import fixed_width_repr_of_int
from pybufrkit.bufr import BufrMessage
from pybufrkit.descriptors import (Descriptor, ElementDescriptor, FixedReplicationDescriptor,
                                   DelayedReplicationDescriptor, OperatorDescriptor,
                                   SequenceDescriptor, SkippedLocalDescriptor,
                                   AssociatedDescriptor, MarkerDescriptor)
from pybufrkit.templatedata import (TemplateData, NoValueDataNode, SequenceNode,
                                    FixedReplicationNode, DelayedReplicationNode)
from pybufrkit.dataquery import QueryResult


class Renderer(object):
    """
    This class is the abstract base Renderer. A renderer provides the contract
    to take in an object and convert it into a string representation.
    """

    def render(self, obj):
        """
        Render the given object as string.

        :param object obj: The object to render
        :return: A string representation of the given object.
        :rtype: str
        """
        if isinstance(obj, BufrMessage):
            return self._render_bufr_message(obj)
        elif isinstance(obj, TemplateData):
            return self._render_template_data(obj)
        elif isinstance(obj, Descriptor):
            return self._render_descriptor(obj)
        elif isinstance(obj, QueryResult):
            return self._render_query_result(obj)
        else:
            raise PyBufrKitError('Unknown object {} for rendering'.format(type(obj)))

    @abc.abstractmethod
    def _render_bufr_message(self, bufr_message):
        """Render a bufr message"""

    @abc.abstractmethod
    def _render_template_data(self, template_data):
        """Render the template data"""

    @abc.abstractmethod
    def _render_descriptor(self, descriptor):
        """Render a Descriptor (including all of its subclasses)"""

    @abc.abstractmethod
    def _render_query_result(self, query_result):
        """Render a QueryResult object"""


class FlatTextRenderer(Renderer):
    """
    This renderer converts the given object by flatten all its sub-structures.
    """

    def _render_bufr_message(self, bufr_message):
        ret = [str(bufr_message.table_group_key)]
        for section in bufr_message.sections:
            ret.append('<<<<<< section {} >>>>>>'.format(section.get_metadata('index')))
            for parameter in section:
                if parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                    ret.extend(
                        self._render_template_data(parameter.value).split('\n')
                    )
                else:
                    ret.append('{} = {!r}'.format(parameter.name, parameter.value))

        return '\n'.join(ret)

    def _render_template_data(self, template_data):
        """
        Format the template data so that they have an user-friend display.
        """
        ret = []
        for idx_subset in range(template_data.n_subsets):
            ret.append('###### subset {} of {} ######'.format(idx_subset + 1, template_data.n_subsets))
            descriptors = template_data.decoded_descriptors_all_subsets[idx_subset]
            bitmap_links = template_data.bitmap_links_all_subsets[idx_subset]
            values = template_data.decoded_values_all_subsets[idx_subset]
            for idx, (descriptor, value) in enumerate(zip(descriptors, values)):
                if value is not None and hasattr(descriptor, 'unit') and descriptor.unit == 'FLAG TABLE':
                    value = (
                        value,
                        [(i + 1) for i, bit in enumerate(
                            '{:0{}b}'.format(value, descriptor.nbits)
                        ) if bit == '1']
                    )

                if idx in bitmap_links:
                    ret.append('{} {:64.64} -> {} {!r}'.format(
                        fixed_width_repr_of_int(idx + 1, 5),
                        self._render_descriptor(descriptor),
                        fixed_width_repr_of_int(bitmap_links[idx] + 1, 6, pad_left=False),
                        value)
                    )
                else:
                    ret.append('{} {:74.74} {!r}'.format(
                        fixed_width_repr_of_int(idx + 1, 5),
                        self._render_descriptor(descriptor),
                        value)
                    )
        return '\n'.join(ret)

    def _render_descriptor(self, descriptor):
        lines = self._render_descriptor_helper(descriptor, '')
        return '\n'.join(lines)

    def _render_query_result(self, query_result):
        lines = []
        for idx_subset in query_result.subset_indices():
            values = query_result.get_values(idx_subset, flat=True)
            lines.append('###### subset {} of {} ######'.format(idx_subset + 1, query_result.n_subsets))
            lines.append(','.join('{!r}'.format(v) for v in values))
        return '\n'.join(lines)

    def _render_descriptor_helper(self, descriptor, indent):
        lines = []

        if isinstance(descriptor, SequenceDescriptor):
            lines.append('{}{} {}'.format(indent, descriptor, descriptor.name))
            for member in descriptor.members:
                lines.extend(self._render_descriptor_helper(member, indent + INDENT_CHARS))

        elif isinstance(descriptor, SkippedLocalDescriptor):
            lines.append('{}{} {} bits'.format(indent, descriptor, descriptor.nbits))

        elif isinstance(descriptor, MarkerDescriptor):
            lines.append('{}{}'.format(indent, descriptor))

        elif isinstance(descriptor, ElementDescriptor):
            lines.append('{}{} {}'.format(indent, descriptor, descriptor.name))

        elif isinstance(descriptor, FixedReplicationDescriptor):
            lines.append('{}{}'.format(indent, descriptor))
            for member in descriptor.members:
                lines.extend(self._render_descriptor_helper(member, indent + INDENT_CHARS))

        elif isinstance(descriptor, DelayedReplicationDescriptor):
            lines.append('{}{}'.format(indent, descriptor))
            lines.extend(self._render_descriptor_helper(descriptor.factor, indent + '.' * len(INDENT_CHARS)))
            for member in descriptor.members:
                lines.extend(self._render_descriptor_helper(member, indent + INDENT_CHARS))

        elif isinstance(descriptor, OperatorDescriptor):
            lines.append('{}{}'.format(indent, descriptor))

        elif isinstance(descriptor, AssociatedDescriptor):
            lines.append('{}{} {} bits'.format(indent, descriptor, descriptor.nbits))

        else:
            raise UnknownDescriptor('{}'.format(descriptor))

        return lines


class FlatJsonRenderer(Renderer):
    """
    This renderer converts the given object to a JSON string by flatten its
    internal structure.
    """

    def _render_bufr_message(self, bufr_message):
        """
        Produce a JSON string for the BUFR message that can be encoded back to
        the binary BUFR message.
        """
        data = []
        for section in bufr_message.sections:
            section_data = []
            for parameter in section:
                if parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                    section_data.append(self._render_template_data(parameter.value))
                else:
                    section_data.append(parameter.value)
            data.append(section_data)

        return data

    def _render_template_data(self, template_data):
        return template_data.decoded_values_all_subsets

    def _render_descriptor(self, descriptor):
        raise NotImplementedError()

    def _render_query_result(self, query_result):
        ret = OrderedDict()
        for idx_subset in query_result.subset_indices():
            ret[idx_subset] = query_result.get_values(idx_subset, flat=True)
        return ret


class NestedJsonRenderer(Renderer):
    """
    The counterpart to NestedTextRenderer but with JSON as output
    """

    def _render_bufr_message(self, bufr_message):
        data = []
        for section in bufr_message.sections:
            section_data = []
            for parameter in section:
                parameter_data = {'name': parameter.name}
                if parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                    parameter_data['value'] = self._render_template_data(parameter.value)
                else:
                    parameter_data['value'] = parameter.value
                section_data.append(parameter_data)
            data.append(section_data)

        # return data
        return data

    def _render_template_data(self, template_data):
        ret = []
        for idx_subset in range(template_data.n_subsets):
            ret.append(
                self._render_template_data_nodes(
                    template_data.decoded_nodes_all_subsets[idx_subset],
                    template_data.decoded_descriptors_all_subsets[idx_subset],
                    template_data.decoded_values_all_subsets[idx_subset],
                )
            )
        return ret

    def _render_query_result(self, query_result):
        ret = OrderedDict()
        for idx_subset in query_result.subset_indices():
            ret[idx_subset] = query_result.get_values(idx_subset)
        return ret

    def _render_template_data_nodes(self, decoded_nodes, decoded_descriptors, decoded_values):
        ret = []
        for decoded_node in decoded_nodes:
            if isinstance(decoded_node, NoValueDataNode):
                n = {'id': str(decoded_node.descriptor),
                     'description': str(decoded_node)}

                if isinstance(decoded_node, SequenceNode):
                    n['members'] = self._render_template_data_nodes(
                        decoded_node.members, decoded_descriptors, decoded_values,
                    )

                elif isinstance(decoded_node, (FixedReplicationNode, DelayedReplicationNode)):
                    if isinstance(decoded_node, FixedReplicationNode):
                        n_repeats = decoded_node.descriptor.n_repeats
                    else:
                        n_repeats = decoded_values[decoded_node.factor.index]
                        n['factor'] = self._render_template_data_value_node(
                            decoded_node.factor, decoded_descriptors, decoded_values,
                        )

                    # Get actual number of members instead of number of items which is
                    # calculated from the descriptor ID. When the structure is fully
                    # nested. The number from the descriptor ID is no longer accurate.
                    n_members = decoded_node.descriptor.n_members

                    n['members'] = []
                    for ir in range(n_repeats):
                        n['members'].append(
                            self._render_template_data_nodes(
                                decoded_node.members[ir * n_members: (ir + 1) * n_members],
                                decoded_descriptors, decoded_values,
                            )
                        )
                        # Nested JSON contains no value node that also has no members,
                        # e.g. certain operator descriptors

            else:  # ValueNode
                n = self._render_template_data_value_node(
                    decoded_node, decoded_descriptors, decoded_values
                )

            ret.append(n)

        return ret

    def _render_template_data_value_node(self, decoded_node, decoded_descriptors, decoded_values,
                                         is_attribute=False):
        descriptor = decoded_descriptors[decoded_node.index]
        value = decoded_values[decoded_node.index]
        if isinstance(descriptor, MarkerDescriptor):
            description = '{:06d}'.format(descriptor.marker_id)
        elif hasattr(descriptor, 'name'):
            description = descriptor.name
        else:
            description = decoded_node.__class__.__name__[:-4]

        ret = {'id': str(descriptor), 'description': description, 'value': value}
        if is_attribute and not isinstance(descriptor, AssociatedDescriptor):
            ret['virtual'] = True
        if hasattr(decoded_node, 'attributes'):
            ret['attributes'] = self._render_template_data_attributed_node(
                decoded_node, decoded_descriptors, decoded_values
            )

        return ret

    def _render_template_data_attributed_node(self, decoded_node, decoded_descriptors, decoded_values):
        ret = []
        for attr_node in decoded_node.attributes:
            ret.append(
                self._render_template_data_value_node(
                    attr_node, decoded_descriptors, decoded_values, is_attribute=True
                )
            )
        return ret


class NestedTextRenderer(Renderer):
    """
    This renderer converts the given object to a text string by honoring all its
    nested sub-structures.
    """

    def _render_bufr_message(self, bufr_message):
        """
        Render the template data in a hierarchical format for the BUFR message.
        """
        ret = [str(bufr_message.table_group_key)]
        for section in bufr_message.sections:
            ret.append('<<<<<< section {} >>>>>>'.format(section.get_metadata('index')))
            for parameter in section:
                if parameter.type == PARAMETER_TYPE_TEMPLATE_DATA:
                    ret.extend(
                        self._render_template_data(parameter.value).split('\n')
                    )
                else:
                    ret.append('{} = {!r}'.format(parameter.name, parameter.value))

        return '\n'.join(ret)

    def _render_template_data(self, template_data):
        """
        Render the template data in a hierarchical format.
        """
        ret = []
        for idx_subset in range(template_data.n_subsets):
            ret.append('###### subset {} of {} ######'.format(idx_subset + 1, template_data.n_subsets))
            ret.extend(
                self._render_template_data_nodes(
                    template_data.decoded_nodes_all_subsets[idx_subset],
                    template_data.decoded_descriptors_all_subsets[idx_subset],
                    template_data.decoded_values_all_subsets[idx_subset],
                    indent=''
                )
            )
        return '\n'.join(ret)

    def _render_descriptor(self, descriptor):
        raise NotImplementedError()

    def _render_template_data_nodes(self, decoded_nodes, decoded_descriptors, decoded_values, indent):
        ret = []
        for decoded_node in decoded_nodes:
            if isinstance(decoded_node, NoValueDataNode):
                ret.append('{}{}'.format(indent, decoded_node))

                if isinstance(decoded_node, SequenceNode):
                    ret.extend(
                        self._render_template_data_nodes(
                            decoded_node.members, decoded_descriptors, decoded_values,
                            indent + INDENT_CHARS
                        )
                    )

                elif isinstance(decoded_node, (FixedReplicationNode, DelayedReplicationNode)):
                    if isinstance(decoded_node, FixedReplicationNode):
                        n_repeats = decoded_node.descriptor.n_repeats
                    else:
                        n_repeats = decoded_values[decoded_node.factor.index]
                        ret.extend(
                            self._render_template_data_value_node(
                                decoded_node.factor, decoded_descriptors, decoded_values,
                                indent + '.' * len(INDENT_CHARS)
                            )
                        )

                    # Get actual number of members instead of number of items which is
                    # calculated from the descriptor ID. When the structure is fully
                    # nested. The number from the descriptor ID is no longer accurate.
                    n_members = decoded_node.descriptor.n_members

                    for ir in range(n_repeats):
                        ret.append('{}# --- {} of {} replications ---'.format(
                            indent + INDENT_CHARS, ir + 1, n_repeats)
                        )
                        ret.extend(
                            self._render_template_data_nodes(
                                decoded_node.members[ir * n_members: (ir + 1) * n_members],
                                decoded_descriptors, decoded_values,
                                indent + INDENT_CHARS
                            )
                        )

            else:  # ValueNode
                ret.extend(
                    self._render_template_data_value_node(
                        decoded_node, decoded_descriptors, decoded_values, indent
                    )
                )

        return ret

    def _render_template_data_value_node(self, decoded_node, decoded_descriptors, decoded_values, indent,
                                         is_attribute=False):
        descriptor = decoded_descriptors[decoded_node.index]
        value = decoded_values[decoded_node.index]
        if isinstance(descriptor, MarkerDescriptor):
            description = '{:06d}'.format(descriptor.marker_id)
        elif hasattr(descriptor, 'name'):
            description = descriptor.name
        else:
            description = decoded_node.__class__.__name__[:-4]

        ret = [
            '{}{}{} {} {!r}'.format(
                indent,
                '-> ' if is_attribute else '',
                descriptor,
                description,
                value
            )
        ]
        if hasattr(decoded_node, 'attributes'):
            ret.extend(
                self._render_template_data_attributed_node(
                    decoded_node, decoded_descriptors, decoded_values, indent + INDENT_CHARS
                )
            )

        return ret

    def _render_template_data_attributed_node(self, decoded_node, decoded_descriptors, decoded_values, indent):
        ret = []
        for attr_node in decoded_node.attributes:
            ret.extend(
                self._render_template_data_value_node(
                    attr_node, decoded_descriptors, decoded_values, indent, is_attribute=True
                )
            )
        return ret
