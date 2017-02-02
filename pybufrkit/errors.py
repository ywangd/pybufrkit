"""
pybufrkit.errors
~~~~~~~~~~~~~~~~
"""


class PyBufrKitError(Exception):
    """
    This is the root exception object of the package.
    """

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