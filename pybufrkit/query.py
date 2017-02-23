"""
pybufrkit.query
~~~~~~~~~~~~~~~
"""
from __future__ import absolute_import
from __future__ import print_function


from pybufrkit.dataquery import NodePathParser, DataQuerent
from pybufrkit.mdquery import MetadataExprParser, MetadataQuerent, METADATA_QUERY_INDICATOR_CHAR


class BufrMessageQuerent(object):
    """
    This is a convenient class for querents of metadata and data sections.
    It provides an uniform interface for querying the BufrMessage object.
    """

    def __init__(self):
        self.metadata_querent = MetadataQuerent(MetadataExprParser())
        self.data_querent = DataQuerent(NodePathParser())

    def query(self, bufr_message, query_expr):
        query_expr = query_expr.lstrip()
        if query_expr[0] == METADATA_QUERY_INDICATOR_CHAR:
            return self.metadata_querent.query(bufr_message, query_expr)
        else:
            return self.data_querent.query(bufr_message, query_expr)
