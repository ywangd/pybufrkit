"""
pybufrkit.errors
~~~~~~~~~~~~~~~~
"""


class PyBufrKitError(Exception):
    """
    This is the root exception object of the package.
    """

    def __init__(self, message=''):
        self.message = message

    def __str__(self):
        return 'Error: {}'.format(self.message)


class UnknownDescriptor(PyBufrKitError):
    """
    An unknown, i.e. not found in BUFR tables, BUFR descriptor.
    """


class BitReadError(PyBufrKitError):
    """
    Bit reading error
    """


class PathExprParsingError(PyBufrKitError):
    """
    Error on parsing a Path expression
    """


class MetadataExprParsingError(PyBufrKitError):
    """
    Error on parsing a metadata variable expression
    """


class QueryError(PyBufrKitError):
    """
    General error on query processing
    """
