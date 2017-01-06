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

from pybufrkit.decoder import Decoder
from pybufrkit.encoder import Encoder
from pybufrkit.descriptors import ElementDescriptor
from pybufrkit.tables import get_table_group
from pybufrkit.renderer import FlatTextRenderer, FlatJsonRenderer, NestedTextRenderer

__version__ = '0.2.0'
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

    ap.add_argument('--debug', action='store_true',
                    help='Set logging level to DEBUG')

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

    ns = ap.parse_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG if ns.debug else logging.ERROR,
        format='%(asctime)s: %(levelname)s: %(funcName)s: %(message)s'
    )

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

            print(flat_text_render.render(bufr_message))
            if ns.template:
                table_group = get_table_group(
                    tables_root_dir=ns.tables_root_directory,
                    master_table_number=bufr_message.master_table_number.value,
                    originating_centre=bufr_message.originating_centre.value,
                    originating_subcentre=bufr_message.originating_subcentre.value,
                    master_table_version=bufr_message.master_table_version.value,
                    local_table_version=bufr_message.local_table_version.value,
                    normalize=1
                )
                template = table_group.template_from_ids(*bufr_message.unexpanded_descriptors.value)
                print(flat_text_render.render(template))

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
                    if ns.code_and_flag and descriptor.unit in ('CODE TABLE', 'FLAG TABLE'):
                        code_and_flag = table_group.B.code_and_flag_for_descriptor(descriptor)
                        if code_and_flag:
                            for v, description in code_and_flag:
                                print(u'{:8d} {}'.format(v, description))
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

    else:
        raise RuntimeError('Unknown sub-command: {}'.format(ns.sub_command))
