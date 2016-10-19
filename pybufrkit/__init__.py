"""
Python Toolkit for BUFR Messages

Decode and encode BUFR messages with Pure Python.
"""
from __future__ import absolute_import
from __future__ import print_function
import sys
import os
import logging
import argparse

from .bpclpp import peek_mode
from .decoder import Decoder
from .encoder import Encoder
from .descriptors import ElementDescriptor

__version__ = '0.1.3'
__author__ = 'ywangd@gmail.com'

# TODO: loggings
LOGGER = logging.getLogger('PyBufrKit')
LOGGER.addHandler(logging.NullHandler())  # so testings do not complain about no handler


def main():
    ap = argparse.ArgumentParser(prog=__package__,
                                 description='Python Toolkit for BUFR Messages',
                                 add_help=False)
    ap.add_argument('-h', '--help', action='help',
                    help='Show this help message and exit')
    ap.add_argument('-v', '--version', action='version',
                    version='{}: {}'.format(__package__, __version__),
                    help="Show program's version number and exit")
    ap.add_argument('-V', '--verbose', action='store_true',
                    help='Be more chatty')

    ap.add_argument('-w', '--working-directory',
                    help='The working directory')

    ap.add_argument('-d', '--definitions-directory',
                    help='The directory to locate definition files')

    ap.add_argument('-t', '--tables-root-directory',
                    help='The directory to locate BUFR tables')

    ap.add_argument('--no-log-to-console',
                    action='store_true',
                    help='Do NOT write logs to stdout')

    subparsers = ap.add_subparsers(
        dest='sub_command',
        title='List of sub-commands',
        metavar='sub-command',
        help='Use "{} sub-command -h" for more help on a sub-command'.format(__package__)
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
    decode_parser.add_argument('-i', '--interactive',
                               action='store_true',
                               help='Enter interactive decode mode (Not Yet Implemented)')
    decode_parser.add_argument('--force-dump',
                               action='store_true',
                               help='In case of error, force to dump values decoded so far')

    encode_parser = subparsers.add_parser('encode',
                                          help='Encode given JSON file to BUFR')
    encode_parser.add_argument('json_filename',
                               metavar='json_file',
                               help='The JSON input file')
    encode_parser.add_argument('output_filename',
                               metavar='output_file',
                               nargs='?', default='out.bufr',
                               help='The output BUFR file, default out.bufr')

    info_parser = subparsers.add_parser('info',
                                        help='Show BUFR file information')

    info_parser.add_argument('filenames', metavar='filename',
                             nargs='+',
                             help='BUFR file to decode')
    info_parser.add_argument('-t', '--template',
                             action='store_true', default=False,
                             help='Show expanded template')

    filter_parser = subparsers.add_parser('filter',
                                          help='Filter through given BUFR files')
    filter_parser.add_argument('filenames', metavar='filename',
                               nargs='+',
                               help='BUFR file to filter (Not Yet Implemented)')

    custom_parser = subparsers.add_parser('custom',
                                          help='Run given definition file')
    custom_parser.add_argument('definition_filename',
                               metavar='definition_file',
                               help='The definition file')
    custom_parser.add_argument('filenames', metavar='filename',
                               nargs='*',
                               help='The BUFR file')

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

    if ns.working_directory:
        compiled_template_dir = os.path.join(ns.working_directory, 'compiled_templates')
        save_compiled_template = True
    else:
        compiled_template_dir = None
        save_compiled_template = False

    if ns.sub_command == 'decode':
        if ns.interactive:
            definition_filename = 'boot-{}.bpcl'.format('empty')
            decoder = Decoder(definitions_dir=ns.definitions_directory,
                              definition_filename=definition_filename,
                              tables_root_dir=ns.tables_root_directory,
                              compiled_template_dir=compiled_template_dir,
                              save_compiled_template=save_compiled_template)
            from .interactive import Shell
            shell = Shell(decoder)
            shell.cmdloop('Python Toolkit for BUFR Messages v{}\n'
                          'NOT YET IMPLEMENTED'.format(__version__))

        else:
            definition_filename = 'boot-{}.bpcl'.format('decode')
            decoder = Decoder(definitions_dir=ns.definitions_directory,
                              definition_filename=definition_filename,
                              tables_root_dir=ns.tables_root_directory,
                              compiled_template_dir=compiled_template_dir,
                              save_compiled_template=save_compiled_template)

            for filename in ns.filenames:
                with open(filename, 'rb') as ins:
                    s = ins.read()

                try:
                    bufr = decoder.decode(s, file_path=filename)

                    if ns.attributed:
                        print(bufr.dumps(with_values=False))
                        print(bufr.wire_data().dumps())
                    elif ns.json:
                        print(bufr.jsons())
                    else:
                        print(bufr.dumps())

                except Exception:
                    if ns.force_dump:
                        print(decoder.dumps())
                    else:
                        import traceback
                        traceback.print_exc()

                    sys.exit(1)

    elif ns.sub_command == 'info':
        definition_filename = 'boot-{}.bpcl'.format('info')
        decoder = Decoder(definitions_dir=ns.definitions_directory,
                          definition_filename=definition_filename,
                          tables_root_dir=ns.tables_root_directory,
                          compiled_template_dir=compiled_template_dir,
                          save_compiled_template=save_compiled_template)

        for filename in ns.filenames:
            with open(filename, 'rb') as ins:
                s = ins.read()
            bufr = decoder.decode(s, file_path=filename)

            print(bufr.dumps(with_values=False))
            if ns.template:
                print(bufr.template.dumps())

    elif ns.sub_command == 'custom':
        definitions_directory, definition_filename = os.path.split(ns.definition_filename)
        mode = peek_mode(ns.definition_filename)
        Worker = Decoder if mode == 'decoder' else Encoder
        worker = Worker(definitions_dir=definitions_directory,
                        definition_filename=definition_filename,
                        tables_root_dir=ns.tables_root_directory,
                        compiled_template_dir=compiled_template_dir,
                        save_compiled_template=save_compiled_template)
        if ns.filenames:
            for filename in ns.filenames:
                with open(filename, 'rb') as ins:
                    s = ins.read()
                    if mode == 'decoder':
                        worker.decode(s, filename)
                    else:
                        worker.encode(s, filename)
        else:
            worker.run()

    elif ns.sub_command == 'encode':
        encoder = Encoder()
        if ns.json_filename != '-':
            with open(ns.json_filename) as ins:
                s = ins.read()
        else:  # read from stdin, this is useful for piping
            s = sys.stdin.read()
        bins = encoder.encode(s)
        if ns.output_filename:
            with open(ns.output_filename, 'wb') as outs:
                bins.tofile(outs)

    elif ns.sub_command in ('lookup', 'compile'):
        from .tables import get_table_group
        table_group = get_table_group(ns.tables_root_directory,
                                      ns.master_table_number,
                                      ns.originating_centre,
                                      ns.originating_subcentre,
                                      ns.master_table_version,
                                      ns.local_table_version)

        if ns.sub_command == 'lookup':
            table_group.B.load_code_and_flag()

            descriptors = table_group.descriptors_from_ids(
                *[d.strip() for d in ns.descriptors.split(',')]
            )

            for descriptor in descriptors:
                if isinstance(descriptor, ElementDescriptor):
                    print('{}, {}, {}, {}, {}'.format(descriptor.dumps(),
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
                    print(descriptor.dumps())

        else:
            from .templatecompiler import template_compiler
            func, tree = template_compiler.compile(table_group.template_from_ids(
                *[d.strip() for d in ns.descriptors.split(',')]
            ), with_ast=True)
            try:
                from astunparse import unparse as dumps
            except ImportError:
                from ast import dump as dumps

            print(dumps(tree))

    else:
        raise NotImplementedError()
