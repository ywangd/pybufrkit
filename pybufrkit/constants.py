"""
pybufrkit.constants
~~~~~~~~~~~~~~~~~~~

Various constants used in the module.

"""
from __future__ import absolute_import
from __future__ import print_function

import sys
import os

# Set the base directory accordingly for PyInstaller
# noinspection PyUnresolvedReferences,PyProtectedMember
BASE_DIR = sys._MEIPASS if getattr(sys, 'frozen', False) else os.path.dirname(__file__)

# Default directory to read the definition JSON files
DEFAULT_DEFINITIONS_DIR = os.path.join(BASE_DIR, 'definitions')

# Default directory to load the BUFR tables
DEFAULT_TABLES_DIR = os.path.join(BASE_DIR, 'tables')

NBITS_PER_BYTE = 8

MESSAGE_START_SIGNATURE = b'BUFR'
MESSAGE_STOP_SIGNATURE = b'7777'

PARAMETER_TYPE_UNEXPANDED_DESCRIPTORS = 'unexpanded_descriptors'
PARAMETER_TYPE_TEMPLATE_DATA = 'template_data'

BITPOS_START = 'bitpos_start'

# A list of numbers that corresponds to missing values for a number of bits up to 64
NUMERIC_MISSING_VALUES = [2 ** i - 1 for i in range(65)]


# Number of bits for represent number of bits used for difference
NBITS_FOR_NBITS_DIFF = 6

UNITS_STRING = 'CCITT IA5'
UNITS_FLAG_TABLE = 'FLAG TABLE'
UNITS_CODE_TABLE = 'CODE TABLE'
UNITS_COMMON_CODE_TABLE_C1 = 'Common CODE TABLE C-1'

INDENT_CHARS = '    '
