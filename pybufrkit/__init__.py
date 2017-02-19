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

import sys
import os
import json
import logging
import argparse
import six

from pybufrkit.constants import UNITS_FLAG_TABLE, UNITS_CODE_TABLE, UNITS_COMMON_CODE_TABLE_C1
from pybufrkit.errors import *
from pybufrkit.decoder import Decoder
from pybufrkit.encoder import Encoder
from pybufrkit.descriptors import ElementDescriptor
from pybufrkit.tables import get_table_group
from pybufrkit.renderer import FlatTextRenderer, FlatJsonRenderer, NestedTextRenderer, NestedJsonRenderer

__version__ = '0.2.1'
__author__ = 'ywangd@gmail.com'

LOGGER = logging.getLogger('PyBufrKit')
LOGGER.addHandler(logging.NullHandler())  # so testings do not complain about no handler


def main():
    ap = argparse.ArgumentParser(prog=__name__,
                                 description='Python Toolkit for BUFR Messages',
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
        dest='sub_command',
        title='List of sub-commands',
        metavar='sub-command',
        help='Use "{} sub-command -h" for more help on a sub-command'.format(__name__)
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
                               help='Wire data to set attributes')
    decode_parser.add_argument('--ignore-value-expectation',
                               action='store_true',
                               help='Do not validate value expectations, e.g. 7777 stop signature')
    decode_parser.add_argument('--compiled-template-cache-max',
                               type=int,
                               help='The maximum number of compiled templates to cache. '
                                    'A value greater than 0 is needed to activate template compilation.')

    encode_parser = subparsers.add_parser('encode',
                                          help='Encode given JSON file to BUFR')
    encode_parser.add_argument('json_filename',
                               metavar='json_file',
                               help='The JSON input file')
    encode_parser.add_argument('output_filename',
                               metavar='output_file',
                               nargs='?', default='out.bufr',
                               help='The output BUFR file, default out.bufr')
    encode_parser.add_argument('--compiled-template-cache-max',
                               type=int,
                               help='The maximum number of compiled templates to cache. '
                                    'A value greater than 0 is needed to activate template compilation.')

    info_parser = subparsers.add_parser('info',
                                        help='Show BUFR file information')
    info_parser.add_argument('filenames', metavar='filename',
                             nargs='+',
                             help='BUFR file to decode')
    info_parser.add_argument('-t', '--template',
                             action='store_true', default=False,
                             help='Show expanded template')

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

    compile_parser.add_argument('descriptors',
                                metavar='descriptors',
                                help='Comma separated descriptor IDs')
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
    script_parser.add_argument('script', nargs='?',
                               help='A script string or filename to load the script')
    script_parser.add_argument('filenames', metavar='filename',
                               nargs='+',
                               help='BUFR file to decode')
    script_parser.add_argument('-f', '--script-file', help='load script from file')
    script_parser.add_argument('-F', '--data-values-flatten-level',
                               type=int,
                               help='The flatten level for data values')
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
        if ns.sub_command == 'decode':
            decoder = Decoder(definitions_dir=ns.definitions_directory,
                              tables_root_dir=ns.tables_root_directory,
                              compiled_template_cache_max=ns.compiled_template_cache_max)

            for filename in ns.filenames:
                with open(filename, 'rb') as ins:
                    s = ins.read()

                bufr_message = decoder.process(s, file_path=filename, wire_template_data=False,
                                               ignore_value_expectation=ns.ignore_value_expectation)

                if ns.attributed:
                    bufr_message.wire()
                    print(NestedTextRenderer().render(bufr_message))
                elif ns.json:
                    print(FlatJsonRenderer().render(bufr_message))
                else:
                    print(FlatTextRenderer().render(bufr_message))

        elif ns.sub_command == 'info':
            flat_text_render = FlatTextRenderer()
            decoder = Decoder(definitions_dir=ns.definitions_directory,
                              tables_root_dir=ns.tables_root_directory)

            for filename in ns.filenames:
                with open(filename, 'rb') as ins:
                    s = ins.read()
                    bufr_message = decoder.process(s, file_path=filename, info_only=True)

                bufr_template, table_group = bufr_message.build_template(
                    ns.tables_root_directory, normalize=1)

                print(flat_text_render.render(bufr_message))
                if ns.template:
                    print(flat_text_render.render(bufr_template))

        elif ns.sub_command == 'encode':
            encoder = Encoder(definitions_dir=ns.definitions_directory,
                              tables_root_dir=ns.tables_root_directory,
                              compiled_template_cache_max=ns.compiled_template_cache_max)
            if ns.json_filename != '-':
                with open(ns.json_filename) as ins:
                    s = ins.read()
            else:  # read from stdin, this is useful for piping
                s = sys.stdin.read()
            bufr_message = encoder.process(s, '<stdin>' if ns.json_filename else ns.json_filename,
                                           wire_template_data=False)
            if ns.output_filename:
                with open(ns.output_filename, 'wb') as outs:
                    outs.write(bufr_message.serialized_bytes)

        elif ns.sub_command in ('lookup', 'compile'):
            table_group = get_table_group(ns.tables_root_directory,
                                          ns.master_table_number,
                                          ns.originating_centre,
                                          ns.originating_subcentre,
                                          ns.master_table_version,
                                          ns.local_table_version)

            if ns.sub_command == 'lookup':
                flat_text_render = FlatTextRenderer()
                table_group.B.load_code_and_flag()  # load the code and flag tables for additional details
                descriptors = table_group.descriptors_from_ids(
                    *[d.strip() for d in ns.descriptors.split(',')]
                )

                for descriptor in descriptors:
                    if isinstance(descriptor, ElementDescriptor):
                        print('{}, {}, {}, {}, {}'.format(flat_text_render.render(descriptor),
                                                          descriptor.unit,
                                                          descriptor.scale,
                                                          descriptor.refval,
                                                          descriptor.nbits))
                        if ns.code_and_flag and descriptor.unit in (UNITS_FLAG_TABLE,
                                                                    UNITS_CODE_TABLE,
                                                                    UNITS_COMMON_CODE_TABLE_C1):
                            code_and_flag = table_group.B.code_and_flag_for_descriptor(descriptor)
                            if code_and_flag:
                                for v, description in code_and_flag:
                                    output = u'{:8d} {}'.format(v, description)
                                    # With Python 2, some terminal utilities, e.g. more, redirect to file,
                                    # cause errors when unicode string is printed. The fix is to encode
                                    # them before print.
                                    if six.PY2:
                                        output = output.encode('utf-8', 'ignore')
                                    print(output)
                    else:
                        print(flat_text_render.render(descriptor))

            else:  # compile
                from pybufrkit.templatecompiler import TemplateCompiler
                template_compiler = TemplateCompiler()
                descriptor_ids = [x.strip() for x in ns.descriptors.split(',')]
                table_group = get_table_group(tables_root_dir=ns.tables_root_directory)
                template = table_group.template_from_ids(*descriptor_ids)
                compiled_template = template_compiler.process(template, table_group)
                print(json.dumps(compiled_template.to_dict()))

        elif ns.sub_command == 'query':
            decoder = Decoder(definitions_dir=ns.definitions_directory,
                              tables_root_dir=ns.tables_root_directory,
                              compiled_template_cache_max=ns.compiled_template_cache_max)

            for filename in ns.filenames:
                with open(filename, 'rb') as ins:
                    s = ins.read()

                if ns.query_string.strip()[0] == '%':
                    bufr_message = decoder.process(s, file_path=filename, info_only=True)
                    from pybufrkit.mdquery import MetadataExprParser, MetadataQuerent
                    querent = MetadataQuerent(MetadataExprParser())
                    value = querent.query(bufr_message, ns.query_string)
                    print(filename)
                    print(value)

                else:
                    bufr_message = decoder.process(s, file_path=filename, wire_template_data=True,
                                                   ignore_value_expectation=ns.ignore_value_expectation)
                    from pybufrkit.dataquery import BasicNodePathParser, DataQuerent
                    querent = DataQuerent(BasicNodePathParser())
                    query_result = querent.query(bufr_message, ns.query_string)
                    if ns.json:
                        if ns.nested:
                            print(NestedJsonRenderer().render(query_result))
                        else:
                            print(FlatJsonRenderer().render(query_result))
                    else:
                        print(filename)
                        print(FlatTextRenderer().render(query_result))

        elif ns.sub_command == 'script':
            from pybufrkit.script import ScriptRunner

            if ns.script_file:
                with open(ns.script) as ins:
                    script_string = ins.read()
            else:
                if ns.script == '-':
                    script_string = sys.stdin.read()
                else:
                    script_string = ns.script

            script_runner = ScriptRunner(script_string,
                                         data_values_flatten_level=ns.data_values_flatten_level)

            decoder = Decoder(definitions_dir=ns.definitions_directory,
                              tables_root_dir=ns.tables_root_directory,
                              compiled_template_cache_max=ns.compiled_template_cache_max)

            for filename in ns.filenames:
                with open(filename, 'rb') as ins:
                    s = ins.read()

                bufr_message = decoder.process(s, file_path=filename, wire_template_data=True,
                                               ignore_value_expectation=ns.ignore_value_expectation,
                                               info_only=script_runner.metadata_only)

                script_runner.run(bufr_message)

        else:
            print('Unknown sub-command: {}'.format(ns.sub_command))

    except (UnknownDescriptor, BitReadError) as e:
        print(e)

    except (PathExprParsingError, QueryError) as e:
        print(e)

    except IOError as e:
        print('Error: {}: {}'.format(e.strerror, e.filename))
