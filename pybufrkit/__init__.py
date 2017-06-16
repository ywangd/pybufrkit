"""
pybufrkit
~~~~~~~~~

Work with WMO BUFR messages with Pure Python.

.. note:: APIs of version 0.2.0 are breaking changes from those of version 0.1.x
          and it is always recommended to upgrade to the latest version.

:github: https://github.com/ywangd/pybufrkit
:docs: http://pybufrkit.readthedocs.io/
:author: Yang Wang (ywangd@gmail.com)
"""
from __future__ import absolute_import
from __future__ import print_function

import argparse
import logging
import sys

from pybufrkit.commands import (command_compile,
                                command_decode,
                                command_encode,
                                command_info,
                                command_lookup,
                                command_query,
                                command_script,
                                command_split,
                                command_subset)
from pybufrkit.errors import (BitReadError,
                              PathExprParsingError,
                              QueryError,
                              UnknownDescriptor,
                              PyBufrKitError)

__version__ = '0.2.5'
__author__ = 'ywangd@gmail.com'

LOGGER = logging.getLogger('PyBufrKit')
LOGGER.addHandler(logging.NullHandler())  # so testings do not complain about no handler


def main():
    ap = argparse.ArgumentParser(prog=__name__,
                                 description='Pure Python Toolkit for BUFR Messages',
                                 add_help=False)
    ap.add_argument('-h', '--help', action='help',
                    help='Show this help message and exit')
    ap.add_argument('-v', '--version', action='version',
                    version='{}: {}'.format(__name__, __version__),
                    help="Show program's version number and exit")

    verbosity_group = ap.add_mutually_exclusive_group()
    verbosity_group.add_argument('--info', action='store_true',
                                 help='Show messages of severity level INFO or higher')
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Show messages of severity level DEBUG or higher')

    ap.add_argument('-d', '--definitions-directory',
                    help='The directory to locate definition files')

    ap.add_argument('-t', '--tables-root-directory',
                    help='The directory to locate BUFR tables')

    subparsers = ap.add_subparsers(
        dest='command',
        title='List of commands',
        metavar='command',
        help='Use "{} command -h" for more help on a command'.format(__name__)
    )

    decode_parser = subparsers.add_parser('decode',
                                          help='decode BUFR file')
    decode_parser.add_argument('filenames', metavar='filename',
                               nargs='+',
                               help='BUFR file to decode')
    decode_parser.add_argument('-j', '--json',
                               action='store_true',
                               help='Output as JSON')
    decode_parser.add_argument('-a', '--attributed',
                               action='store_true',
                               help='Wire data to be attributed and nested')
    decode_parser.add_argument('-m', '--multiple-messages',
                               action='store_true',
                               help='Each given file could have one or more messages')
    decode_parser.add_argument('--ignore-value-expectation',
                               action='store_true',
                               help='Do not validate value expectations, e.g. 7777 stop signature')
    decode_parser.add_argument('--compiled-template-cache-max',
                               type=int,
                               help='The maximum number of compiled templates to cache. '
                                    'A value greater than 0 is needed to activate template compilation.')

    encode_parser = subparsers.add_parser('encode',
                                          help='Encode given JSON file to BUFR')
    encode_parser.add_argument('filename',
                               metavar='input',
                               help='The input file')
    encode_parser.add_argument('output_filename',
                               metavar='output_file',
                               nargs='?', default='out.bufr',
                               help='The output BUFR file, default out.bufr')
    encode_parser.add_argument('-j', '--json', action='store_true',
                               help='The input is in JSON format')
    encode_parser.add_argument('-a', '--attributed', action='store_true',
                               help='The input takes nested and attributed format')
    encode_parser.add_argument('--compiled-template-cache-max',
                               type=int,
                               help='The maximum number of compiled templates to cache. '
                                    'A value greater than 0 is needed to activate template compilation.')

    info_parser = subparsers.add_parser('info',
                                        help='Show BUFR file information')
    info_parser.add_argument('filenames', metavar='filename',
                             nargs='+',
                             help='BUFR files to decode')
    info_parser.add_argument('-t', '--template',
                             action='store_true', default=False,
                             help='Show expanded template')
    info_parser.add_argument('-m', '--multiple-messages',
                             action='store_true',
                             help='Each given file could have one or more messages')
    info_parser.add_argument('-c', '--count-only', action='store_true',
                             help='Only count number of messages in the file')

    split_parser = subparsers.add_parser(
        'split',
        help='Split given files so each file contains a single BUFR Message per file.')
    split_parser.add_argument('filenames', metavar='filename', nargs='+',
                              help='BUFR files to split')

    lookup_parser = subparsers.add_parser(
        'lookup',
        help='Lookup information for the given descriptor(s)'
    )
    lookup_parser.add_argument('descriptors',
                               metavar='descriptors',
                               help='Comma separated descriptor IDs')
    lookup_parser.add_argument('--master-table-number',
                               help='The master BUFR table Number')
    lookup_parser.add_argument('--originating-centre',
                               help='Code of the original centre')
    lookup_parser.add_argument('--originating-subcentre',
                               help='Code of the originating subcentre')
    lookup_parser.add_argument('--master-table-version',
                               help='The master BUFR table version')
    lookup_parser.add_argument('--local-table-version',
                               help='The local table version')

    lookup_parser.add_argument('-l', '--code-and-flag',
                               action='store_true',
                               help='show code and flag list')

    compile_parser = subparsers.add_parser(
        'compile',
        help='Compile the given BUFR Template (comma separated descriptor IDs)'
    )

    compile_parser.add_argument('input',
                                metavar='input',
                                help='BUFR file or Comma separated descriptor IDs')
    compile_parser.add_argument('--master-table-number',
                                help='The master BUFR table Number')
    compile_parser.add_argument('--originating-centre',
                                help='Code of the original centre')
    compile_parser.add_argument('--originating-subcentre',
                                help='Code of the originating subcentre')
    compile_parser.add_argument('--master-table-version',
                                help='The master BUFR table version')
    compile_parser.add_argument('--local-table-version',
                                help='The local table version')

    subset_parser = subparsers.add_parser('subset', help='Subset BUFR messages')
    subset_parser.add_argument('subset_indices',
                               help='A comma separate list of subset indices (zero based)')
    subset_parser.add_argument('filename', metavar='filename',
                               help='BUFR file to subset')
    subset_parser.add_argument('output_filename',
                               metavar='output_file',
                               nargs='?', default='out.bufr',
                               help='The output BUFR file, default out.bufr')
    subset_parser.add_argument('--ignore-value-expectation',
                               action='store_true',
                               help='Do not validate value expectations, e.g. 7777 stop signature')
    subset_parser.add_argument('--compiled-template-cache-max',
                               type=int,
                               help='The maximum number of compiled templates to cache. '
                                    'A value greater than 0 is needed to activate template compilation.')

    query_parser = subparsers.add_parser('query', help='Query metadata and data of BUFR messages')
    query_parser.add_argument('query_string',
                              help='A query string')
    query_parser.add_argument('filenames', metavar='filename',
                              nargs='+',
                              help='BUFR file to decode')
    query_parser.add_argument('-j', '--json',
                              action='store_true',
                              help='Output as JSON')
    query_parser.add_argument('-n', '--nested',
                              action='store_true',
                              help='Output as nested JSON')
    query_parser.add_argument('--ignore-value-expectation',
                              action='store_true',
                              help='Do not validate value expectations, e.g. 7777 stop signature')
    query_parser.add_argument('--compiled-template-cache-max',
                              type=int,
                              help='The maximum number of compiled templates to cache. '
                                   'A value greater than 0 is needed to activate template compilation.')

    script_parser = subparsers.add_parser('script', help='Run script against BUFR messages')
    script_parser.add_argument('input',
                               help='A script string or filename to load the script (use - for stdin)')
    script_parser.add_argument('filenames', metavar='filename',
                               nargs='+',
                               help='BUFR file to decode')
    script_parser.add_argument('-f', '--from-file', action='store_true',
                               help='load script from file')
    script_parser.add_argument('-n', '--data-values-nest-level',
                               type=int,
                               choices=(0, 1, 2, 4),
                               help='The level of list nesting for data values')
    script_parser.add_argument('--ignore-value-expectation',
                               action='store_true',
                               help='Do not validate value expectations, e.g. 7777 stop signature')
    script_parser.add_argument('--compiled-template-cache-max',
                               type=int,
                               help='The maximum number of compiled templates to cache. '
                                    'A value greater than 0 is needed to activate template compilation.')

    ns = ap.parse_args()

    if ns.info:
        logging_level = logging.INFO
    elif ns.debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.WARN

    logging.basicConfig(
        stream=sys.stdout,
        level=logging_level,
        # format='%(asctime)s: %(levelname)s: %(funcName)s: %(message)s'
        format='%(levelname)s: %(message)s'
    )

    try:
        if ns.command == 'decode':
            command_decode(ns)

        elif ns.command == 'info':
            command_info(ns)

        elif ns.command == 'encode':
            command_encode(ns)

        elif ns.command == 'split':
            command_split(ns)

        elif ns.command == 'lookup':
            command_lookup(ns)

        elif ns.command == 'compile':
            command_compile(ns)

        elif ns.command == 'subset':
            command_subset(ns)

        elif ns.command == 'query':
            command_query(ns)

        elif ns.command == 'script':
            command_script(ns)

        else:
            print('Unknown command: {}'.format(ns.command))

    except (UnknownDescriptor, BitReadError) as e:
        print(e)

    except (PathExprParsingError, QueryError) as e:
        print(e)

    except PyBufrKitError as e:
        print(e)

    except IOError as e:
        print('Error: {}: {}'.format(e.strerror, e.filename))
