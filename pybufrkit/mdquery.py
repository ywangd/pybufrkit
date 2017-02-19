"""
pybufrkit.mdquery
~~~~~~~~~~~~~~~~~
"""
from __future__ import absolute_import
from __future__ import print_function

import logging

from pybufrkit.errors import MetadataExprParsingError

__all__ = ['MetadataExprParser', 'MetadataQuerent', 'METADATA_QUERY_INDICATOR_CHAR']

log = logging.getLogger(__file__)

METADATA_QUERY_INDICATOR_CHAR = '%'


class MetadataExprParser(object):
    def parse(self, metadata_expr):
        """

        :param str metadata_expr: The metadata expression string to parse
        :return: A 2-element tuple of section index and metadata name
        :rtype: (int, str)
        """
        metadata_expr = metadata_expr.strip()
        if metadata_expr[0] != METADATA_QUERY_INDICATOR_CHAR:
            raise MetadataExprParsingError('Metadata expression must start with "%"')

        if '.' in metadata_expr:
            section_index, metadata_name = metadata_expr[1:].split('.')
            try:
                section_index = int(section_index)
            except ValueError:
                raise MetadataExprParsingError('Invalid section index: {}'.format(section_index))

        else:
            section_index = None
            metadata_name = metadata_expr[1:]

        return section_index, metadata_name


class MetadataQuerent(object):
    """
    :param MetadataExprParser metadata_expr_parser: Parser for metadata expression
    """

    def __init__(self, metadata_expr_parser):

        self.metadata_expr_parser = metadata_expr_parser

    def query(self, bufr_message, metadata_expr):
        section_index, metadata_name = self.metadata_expr_parser.parse(metadata_expr)
        sections = [s for s in bufr_message.sections
                    if s.get_metadata('index') == section_index or section_index is None]
        for section in sections:
            for parameter in section:
                if parameter.name == metadata_name:
                    return parameter.value

        return None
