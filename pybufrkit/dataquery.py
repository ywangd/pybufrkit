"""
pybufrkit.dataquery
~~~~~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import string
from collections import namedtuple, OrderedDict

from pybufrkit.errors import PathExprParsingError, QueryError
from pybufrkit.templatedata import (
    ValueDataNode,
    FixedReplicationNode, DelayedReplicationNode, SequenceNode
)

__all__ = ['NodePathParser', 'DataQuerent', 'QueryResult', 'PATH_SEPARATOR_CHILD']

_PathComponent = namedtuple('PathComponent', ['separator', 'id', 'slice'])

log = logging.getLogger(__file__)


class PathComponent(_PathComponent):
    """
    This class represents a Path Component that is used to located a Node
    in the hierarchical wired node tree. It has three components, path separator, id
    and slice.

    Path separator describes the relationship of this path component to its
    predecessor (if any).
        * A Slash (/) means the component is a child/member of its predecessor
        * A Dot (.) means the component is an attribute of of its predecessor
        * A Greater Than (>) character means the component is a descendant
          of its predecessor.

    ID is the entity's ID. If there are multiple matches, the slice is to limit
    the results.
    """


class NodePath(object):
    """
    This class represents a complete path that can be used to uniquely identify
    a BUFR Data node.
    """

    def __init__(self, path_string):
        self.path_string = path_string
        self.subset_slice = None
        self.components = []

    def __str__(self):
        ret = '' if self.subset_slice is None else '@{}'.format(self.slice_to_str(self.subset_slice))

        for component in self.components:
            ret += '{}{}{}'.format(
                component.separator, component.id, self.slice_to_str(component.slice)
            )
        return ret

    def slice_to_str(self, slc):
        return '[{}]'.format(slc) if not isinstance(slc, slice) else '[{}:{}:{}]'.format(
            slc.start if slc.start is not None else '',
            slc.stop if slc.stop is not None else '',
            slc.step if slc.step is not None else '',
        )

    def add_component(self, component):
        self.components.append(component)


def unexpected_char_error(c, idx):
    return PathExprParsingError('unexpected char: {!r} at position {}'.format(c, idx))


PATH_SEPARATOR_CHILD = '/'
PATH_SEPARATOR_ATTRIB = '.'
PATH_SEPARATOR_DESCEND = '>'

STATE_START_PARSING = ''
STATE_START_SUBSET = '@'
STATE_START_SUBSET_SLICE_0 = '@['
STATE_START_SUBSET_SLICE_X = '@:'
STATE_STOP_SUBSET_SLICE = '@]'
STATE_START_ID = 'i'
STATE_START_SLICE_0 = '['
STATE_START_SLICE_X = ':'
STATE_STOP_SLICE = ']'


# noinspection PyAttributeOutsideInit
class NodePathParser(object):
    """
    This class provides a parser for parsing path query string.

    :param bool bare_id_matches_all: By default, a path component with
        bare ID, i.e. with no slicing part, means match all occurrences,
        i.e. [::]. If set to False, it only matches the first occurence.
    """

    def __init__(self, bare_id_matches_all=True):
        self.bare_id_matches_all = bare_id_matches_all

    def reset(self):
        self.pos = 0
        self.current_state = None
        self.current_token = None
        self.current_id = None
        self.current_separator = None
        self.current_slice_elements = []

    def parse(self, path_expr):
        path_expr_stripped = path_expr.strip()

        if path_expr_stripped == '':
            raise PathExprParsingError('Empty path expression')

        # A path expression should always start with one of the following chars
        # Otherwise fail fast.
        if path_expr_stripped[0] not in '@/>0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            raise unexpected_char_error(path_expr_stripped[0], path_expr.find(path_expr_stripped[0]))

        self.reset()
        self.node_path = NodePath(path_expr)

        self.current_state = STATE_START_PARSING
        self.current_token = ''
        while self.pos < len(path_expr):
            c = path_expr[self.pos]

            if c in string.whitespace:
                pass  # all whitespaces are ignored

            elif c == '@':  # start of subset specifier
                if self.current_state == STATE_START_PARSING:
                    self.current_state = STATE_START_SUBSET
                else:
                    raise unexpected_char_error(c, self.pos)

            elif c == '[':
                self.handle_left_bracket()

            elif c in (':', ']'):
                self.handle_colon_and_right_bracket(c)

            elif c in (PATH_SEPARATOR_CHILD, PATH_SEPARATOR_ATTRIB, PATH_SEPARATOR_DESCEND):
                self.handle_separator(c)

            else:
                if self.current_state in (STATE_START_ID,
                                          STATE_START_SUBSET_SLICE_0,
                                          STATE_START_SUBSET_SLICE_X,
                                          STATE_START_SLICE_0,
                                          STATE_START_SLICE_X):
                    self.current_token += c

                elif self.current_state == STATE_START_PARSING:
                    self.handle_separator(PATH_SEPARATOR_DESCEND)
                    self.current_token += c

                else:
                    raise unexpected_char_error(c, self.pos)

            self.pos += 1

        if self.current_state == STATE_START_ID:
            self.current_id = self.convert_id()
            self.add_new_path_component()

        elif self.current_state == STATE_STOP_SLICE:
            self.add_new_path_component()

        elif self.current_token != '':
            raise unexpected_char_error(self.current_token[0], self.pos - len(self.current_token))

        return self.node_path

    def handle_left_bracket(self):
        if self.current_state == STATE_START_SUBSET:
            self.current_state = STATE_START_SUBSET_SLICE_0

        elif self.current_state == STATE_START_ID:
            self.current_state = STATE_START_SLICE_0
            self.current_id = self.convert_id()

        else:
            raise unexpected_char_error('[', self.pos)

    def handle_colon_and_right_bracket(self, c):
        if self.current_state not in (STATE_START_SLICE_0,
                                      STATE_START_SLICE_X,
                                      STATE_START_SUBSET_SLICE_0,
                                      STATE_START_SUBSET_SLICE_X):
            raise unexpected_char_error(c, self.pos)

        if (c == ']' and self.current_token == '' and
            self.current_state in (STATE_START_SLICE_0,
                                       STATE_START_SUBSET_SLICE_0)):
            raise unexpected_char_error(c, self.pos)

        self.current_slice_elements.append(self.convert_slice_element())

        if c == ':':
            if self.current_state == STATE_START_SUBSET_SLICE_0:
                self.current_state = STATE_START_SUBSET_SLICE_X
            elif self.current_state == STATE_START_SLICE_0:
                self.current_state = STATE_START_SLICE_X
        else:  # ']'
            if self.current_state in [STATE_START_SUBSET_SLICE_0,
                                      STATE_START_SUBSET_SLICE_X]:
                self.current_state = STATE_STOP_SUBSET_SLICE
            else:
                self.current_state = STATE_STOP_SLICE

    def handle_separator(self, c):
        if self.current_state == STATE_START_PARSING and c != PATH_SEPARATOR_ATTRIB:
            self.node_path.subset_slice = self.create_slice_object()
            self.current_state = STATE_START_ID

        elif self.current_state == STATE_START_ID:
            self.current_id = self.convert_id()
            self.add_new_path_component()

        elif self.current_state == STATE_STOP_SUBSET_SLICE:
            if c == PATH_SEPARATOR_ATTRIB:
                raise unexpected_char_error(c, self.pos)
            self.node_path.subset_slice = self.create_slice_object()

        elif self.current_state == STATE_STOP_SLICE:
            self.add_new_path_component()

        else:
            raise unexpected_char_error(c, self.pos)

        self.current_separator = c
        self.current_state = STATE_START_ID

    def convert_slice_element(self):
        try:
            ret = None if self.current_token == '' else int(self.current_token)
            self.current_token = ''
            return ret
        except ValueError:
            raise PathExprParsingError('invalid slice syntax: {!r} at position {}'.format(self.current_token, self.pos))

    def convert_id(self, ):
        if self.current_token == '':
            raise PathExprParsingError('empty ID at position {}'.format(self.pos))

        token, self.current_token = self.current_token, ''
        return token

    def create_slice_object(self):
        if len(self.current_slice_elements) == 0:
            if self.bare_id_matches_all:
                slc_obj = slice(None, None, None)
            else:
                slc_obj = 0
        elif len(self.current_slice_elements) == 1:
            # This must be true or the parsing logic is wrong
            assert isinstance(self.current_slice_elements[0], int)
            if self.current_slice_elements[0] >= 0:
                slc_obj = self.current_slice_elements[0]
            else:
                slc_obj = slice(self.current_slice_elements[0],
                                self.current_slice_elements[0] + 1 if self.current_slice_elements[0] != -1 else None,
                                None)
        elif len(self.current_slice_elements) <= 3:  # 2 or 3
            slc_obj = slice(*self.current_slice_elements)
        else:
            raise PathExprParsingError('slice can have at most three indices')

        self.current_slice_elements = []
        return slc_obj

    def add_new_path_component(self):
        slc_obj = self.create_slice_object()
        self.node_path.add_component(
            PathComponent(self.current_separator, self.current_id, slc_obj)
        )


class QueryResult(object):
    """
    This class represents the query result.
    """

    def __init__(self, path_expr=''):
        self.path_expr = path_expr
        self.results = OrderedDict()

    def add_subset(self, i_subset, values):
        self.results[i_subset] = values

    def subset_indices(self):
        return list(self.results.keys())

    def get_values(self, i_subset, flat=False):
        values = self.results[i_subset]
        return self.flatten_values(values) if flat else values

    def all_values(self, flat=False):
        if flat:
            return [self.get_values(i, flat=True) for i in self.subset_indices()]
        else:
            return list(self.results.values())

    def __iter__(self):
        return iter(self.results.viewitems())

    def flatten_values(self, values):
        """
        Flatten values as a list with no nesting.
        :param values:
        :return:
        """
        flat_values = []
        for entry in values:
            if isinstance(entry, list):
                flat_values += self.flatten_values(entry)
            else:
                flat_values.append(entry)
        return flat_values


NODE_NOT_MATCH = 0
NODE_MATCH = 1
NODE_KEEP = 2


class DataQuerent(object):
    """
    This class provides interface to query the BUFR Data section.
    """

    def __init__(self, path_parser):
        self.path_parser = path_parser

    # noinspection PyTypeChecker
    def query(self, bufr_message, path_expr):
        """
        Entry method of the class. Query the data section of the given BUFR message
        with the query string.

        :param bufr_message: A BufrMessage object with wired nodes
        :param path_expr: A query string for data.
        :return: A QueryResult object
        """
        node_path = self.path_parser.parse(path_expr)

        subset_indices = (
            [node_path.subset_slice] if isinstance(node_path.subset_slice, int)
            else list(range(bufr_message.n_subsets.value))[node_path.subset_slice]
        )

        template_data = bufr_message.template_data.value
        if bufr_message.is_compressed.value:
            query_result = self.query_compressed_data(template_data, node_path, subset_indices)

        else:
            query_result = self.query_uncompressed_data(template_data, node_path, subset_indices)

        query_result.path_expr = path_expr
        query_result.n_subsets = template_data.n_subsets

        return query_result

    def query_compressed_data(self, template_data, node_path, subset_indices):
        path_query_result = QueryResult()

        nodes = self.process_one_subset(
            template_data.decoded_nodes_all_subsets[0],
            node_path
        )
        for i_subset in subset_indices:
            decoded_values = template_data.decoded_values_all_subsets[i_subset]
            values = self.create_values_from_nodes(nodes, decoded_values)
            path_query_result.add_subset(i_subset, values)

        return path_query_result

    def query_uncompressed_data(self, template_data, node_path, subset_indices):
        path_query_result = QueryResult()

        for i_subset in subset_indices:
            decoded_values = template_data.decoded_values_all_subsets[i_subset]

            nodes = self.process_one_subset(
                template_data.decoded_nodes_all_subsets[i_subset],
                node_path
            )
            values = self.create_values_from_nodes(nodes, decoded_values)
            path_query_result.add_subset(i_subset, values)

        return path_query_result

    def create_values_from_nodes(self, nodes, decoded_values):
        """
        Process through the nested matching node list and create an values list of
        identical structure. This method is recursive.

        :param nodes: A nested list of matching nodes.
        :param decoded_values:
        :return: A nested values list corresponding to the given nodes.
        """
        values = []
        for entry in nodes:
            if isinstance(entry, list):
                values.append(self.create_values_from_nodes(entry, decoded_values))
            else:
                if isinstance(entry, ValueDataNode):
                    values.append(decoded_values[entry.index])
                else:
                    raise QueryError('cannot query valueless node: {}'.format(entry.descriptor))

        return values

    def process_one_subset(self, decoded_nodes, node_path):
        # Create a wrapper root node so it can be passed to the filter_for_sub_nodes method
        virtual_root_node = SequenceNode('TEMPLATE')
        virtual_root_node.members = decoded_nodes

        sub_nodes = self.filter_for_sub_nodes(virtual_root_node, node_path.components)

        return sub_nodes

    def filter_for_sub_nodes(self, node, path_components):
        """
        For the given node, filter through its sub-nodes, which could be child,
        attribute, factor or descendant nodes depending on the separator value
        of the first member of path components. Note that the filtering will
        be performed in a depth first fashion, i.e. the filtering is continued
        with the direct sub-nodes down to the leaves of the node tree or the end
        of path components, whichever encounters first.

        :param node: The node for which the sub-nodes will be filtered
        :param path_components: A list of path components used to filter the nodes.
        :return: A list of qualified nodes matching through the entire path components.
        """

        path_component = path_components[0]

        sub_nodes = []
        if path_component.separator == PATH_SEPARATOR_CHILD:
            sub_nodes += self.filter_for_child_sub_nodes(node, path_components)

        elif path_component.separator == PATH_SEPARATOR_ATTRIB:
            sub_nodes += self.filter_for_attribute_sub_nodes(node, path_components)

        else:  # descendant
            sub_nodes += self.filter_for_descendant_sub_nodes(node, path_components)

        return sub_nodes

    def filter_for_child_sub_nodes(self, node, path_components):
        """
        This method is a specific version of the filter_for_sub_nodes method. It
        first filters through the child nodes of the given node and then goes
        depth first till all the path components are matched or zero match is
        found.
        """

        log.debug('filter child sub-nodes for {}'.format(node))
        if not hasattr(node, 'members'):
            raise QueryError('{} has no child nodes'.format(node.descriptor))

        path_component = path_components[0]

        if isinstance(node, (FixedReplicationNode, DelayedReplicationNode)):

            # Return if no actual members, i.e. delayed replication has factor of zero
            if len(node.members) == 0:
                return []

            matched_indices = self.filter_for_indices(
                node.members[:node.descriptor.n_members], path_component,
            )

            if not matched_indices:
                return []

            replication_envelope = []
            for i in range(0, len(node.members), node.descriptor.n_members):
                member_nodes = node.members[i: i + node.descriptor.n_members]
                sub_nodes = [member_nodes[i] for i in matched_indices]

                if path_component.separator == PATH_SEPARATOR_DESCEND:
                    sub_nodes = self.descend_and_proceed(sub_nodes, path_components)
                else:
                    sub_nodes = self.proceed_next_path_component(sub_nodes, path_components)

                if sub_nodes:
                    replication_envelope.append(sub_nodes)

            return [replication_envelope] if replication_envelope else []

        else:
            sub_nodes = self.filter_for_nodes(node.members, path_component)
            if sub_nodes:
                if path_component.separator == PATH_SEPARATOR_DESCEND:
                    sub_nodes = self.descend_and_proceed(sub_nodes, path_components)
                else:
                    sub_nodes = self.proceed_next_path_component(sub_nodes, path_components)

            return sub_nodes

    def filter_for_attribute_sub_nodes(self, node, path_components):
        """
        This method is a specific version of the filter_for_sub_nodes method. It
        first filters through the attribute nodes of the given node and then goes
        depth first till all the path components are matched or zero match is
        found.
        """

        log.debug('filter attribute sub-nodes for {}'.format(node))
        if not (hasattr(node, 'attributes') or hasattr(node, 'factor')):
            raise QueryError('{} has no attribute nodes'.format(node.descriptor))

        path_component = path_components[0]

        sub_nodes = []
        if isinstance(node, DelayedReplicationNode):
            sub_nodes += self.filter_for_nodes([node.factor], path_component)

        if hasattr(node, 'attributes'):
            sub_nodes += self.filter_for_nodes(node.attributes, path_component)

        if sub_nodes:
            if path_component.separator == PATH_SEPARATOR_DESCEND:
                sub_nodes = self.descend_and_proceed(sub_nodes, path_components)
            else:
                sub_nodes = self.proceed_next_path_component(sub_nodes, path_components)

        return sub_nodes

    def filter_for_descendant_sub_nodes(self, node, path_components):
        """
        This method is a specific version of the filter_for_sub_nodes method. It
        filter through all descendant nodes in a depth first fashion of the
        given node. A descendant node could be either a child, attribute or
        factor node all the way to the leaf node. It then process through path
        components till every component is matched or zero match is encountered.
        """
        log.debug('filter descendant sub-nodes for {}'.format(node))
        if not (hasattr(node, 'members') or hasattr(node, 'attributes') or hasattr(node, 'factor')):
            raise QueryError('{} has no descendant nodes'.format(node.descriptor))

        sub_nodes = []
        if hasattr(node, 'members'):
            child_sub_nodes = self.filter_for_child_sub_nodes(node, path_components)
            sub_nodes += child_sub_nodes

        if hasattr(node, 'attributes') or hasattr(node, 'factor'):
            attrib_sub_nodes = self.filter_for_attribute_sub_nodes(node, path_components)
            sub_nodes += attrib_sub_nodes

        return sub_nodes

    def descend_and_proceed(self, nodes, path_components):
        """
        Processing through the given list of nodes, for any nodes that are not a direct
        match of the path component, descent to its sub-nodes for further matching. Once
        a match is found, it then proceed through the path component till all the component
        is matched or zero match is encountered.

        :param nodes: A list of nodes to descend into its sub-nodes
        :param path_components: The path components used for matching.
        """
        log.debug('descent into: {}'.format(nodes))
        path_component = path_components[0]

        descend_sub_nodes = []
        for n in nodes:
            match = self.node_matches(n, path_component)
            if match == NODE_KEEP:
                # If it is a match due to being a composite node, descend to its sub-nodes
                # for any match of the current path component
                sub_nodes = self.filter_for_descendant_sub_nodes(n, path_components)
                descend_sub_nodes += sub_nodes
            elif match == NODE_MATCH:
                # If it is an exact match, proceed further through the path components
                sub_nodes = self.proceed_next_path_component([n], path_components)
                descend_sub_nodes += sub_nodes

        return descend_sub_nodes

    def proceed_next_path_component(self, nodes, path_components):
        """
        Proceed further down the path components.

        :param nodes:
        :param path_components:
        :return:
        """
        if len(path_components) > 1:
            sub_nodes = []
            for n in nodes:
                sub_nodes += self.filter_for_sub_nodes(n, path_components[1:])
            return sub_nodes
        else:
            return nodes

    def filter_for_nodes(self, nodes, path_component):
        """
        Filter the given list of nodes using the path component. Note this method is
        different from the filter_for_sub_nodes method in that it filters the given
        nodes themselves, NOT their sub-nodes. The return value will be a selection
        of the given nodes.

        :param nodes: A list of nodes to be filtered
        :param path_component: The path component used for the filtering.
        :return: A list of nodes that qualified by the path component.
        """
        return self.filter_for_entities(
            nodes, path_component, get_node=True
        )

    def filter_for_indices(self, nodes, path_component):
        return self.filter_for_entities(
            nodes, path_component, get_node=False
        )

    def filter_for_entities(self, nodes, path_component, get_node=True):
        nodes_kept = []
        nodes_matched = []
        for idx, node in enumerate(nodes):
            match = self.node_matches(node, path_component)
            if match == NODE_KEEP:
                nodes_kept.append((idx, node))

            elif match == NODE_MATCH:
                nodes_matched.append((idx, node))

            if path_component.separator != PATH_SEPARATOR_DESCEND and \
                    isinstance(path_component.slice, int) and path_component.slice < len(nodes_matched):
                return [nodes_matched[-1][1] if get_node else nodes_matched[-1][0]]

        if isinstance(path_component.slice, int):
            filtered_nodes = (
                [nodes_matched[path_component.slice]] + nodes_kept
                if path_component.slice < len(nodes_matched) else nodes_kept
            )
        else:
            filtered_nodes = nodes_matched[path_component.slice] + nodes_kept

        return [
            (node if get_node else idx)
            for (idx, node) in sorted(filtered_nodes, key=lambda x: x[0])
        ]

    def node_matches(self, node, path_component):
        """
        Check whether the given node is qualified with the path component. If the path
        component's separator is descendant, any sub-nodes containing node is qualified.

        :param node:
        :param path_component:
        :return: True or False
        """
        matched = str(node.descriptor) == path_component.id

        # If this is descendant path component, any composite node is a possible
        # candidate as there might matches with its descendant nodes.
        if not matched and path_component.separator == PATH_SEPARATOR_DESCEND:
            matched = (
                hasattr(node, 'members') or hasattr(node, 'attributes') or hasattr(node, 'factor')
            )
            return NODE_KEEP if matched else NODE_NOT_MATCH

        else:
            return NODE_MATCH if matched else NODE_NOT_MATCH
