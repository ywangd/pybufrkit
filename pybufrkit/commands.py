"""
pybufrkit.commands
~~~~~~~~~~~~~~~~~~

This file gathers all the functions that support the command line usages.
"""
from __future__ import print_function
from __future__ import absolute_import

import os
import sys
import json
import six

from pybufrkit.constants import (UNITS_CODE_TABLE,
                                 UNITS_COMMON_CODE_TABLE_C1,
                                 UNITS_FLAG_TABLE)
from pybufrkit.errors import PyBufrKitError
from pybufrkit.utils import nested_json_to_flat_json, flat_text_to_flat_json, nested_text_to_flat_json
from pybufrkit.descriptors import ElementDescriptor
from pybufrkit.tables import TableGroupCacheManager
from pybufrkit.decoder import Decoder, generate_bufr_message
from pybufrkit.encoder import Encoder
from pybufrkit.utils import JSON_DUMPS_KWARGS
from pybufrkit.renderer import FlatTextRenderer, NestedTextRenderer, FlatJsonRenderer, NestedJsonRenderer

__all__ = ['command_decode', 'command_info', 'command_encode',
           'command_lookup', 'command_compile',
           'command_subset', 'command_query', 'command_script',
           'command_split']


def command_decode(ns):
    """
    Command to decode given files from command line.
    """
    decoder = Decoder(definitions_dir=ns.definitions_directory,
                      tables_root_dir=ns.tables_root_directory,
                      compiled_template_cache_max=ns.compiled_template_cache_max)

    def show_message(m):
        if ns.attributed:
            m.wire()
            if ns.json:
                print(json.dumps(NestedJsonRenderer().render(m), **JSON_DUMPS_KWARGS))
            else:
                print(NestedTextRenderer().render(m))
        else:
            if ns.json:
                print(json.dumps(FlatJsonRenderer().render(m), **JSON_DUMPS_KWARGS))
            else:
                print(FlatTextRenderer().render(m))

    for filename in ns.filenames:
        if filename != '-':
            with open(filename, 'rb') as ins:
                s = ins.read()
        else:
            s = sys.stdin.read()

        if ns.multiple_messages:
            for bufr_message in generate_bufr_message(decoder, s,
                                                      continue_on_error=ns.continue_on_error,
                                                      file_path=filename, wire_template_data=False,
                                                      ignore_value_expectation=ns.ignore_value_expectation,
                                                      filter_expr=ns.filter):
                show_message(bufr_message)
        else:

            bufr_message = decoder.process(s, file_path=filename, wire_template_data=False,
                                           ignore_value_expectation=ns.ignore_value_expectation)
            show_message(bufr_message)


def command_info(ns):
    """
    Command to show metadata information of given files from command line.
    """
    flat_text_render = FlatTextRenderer()
    decoder = Decoder(definitions_dir=ns.definitions_directory,
                      tables_root_dir=ns.tables_root_directory)

    def show_message_info(m):
        bufr_template, table_group = m.build_template(
            ns.tables_root_directory, normalize=1)

        print(flat_text_render.render(m))
        if ns.template:
            print(flat_text_render.render(bufr_template))

    for filename in ns.filenames:
        with open(filename, 'rb') as ins:
            s = ins.read()

        if ns.multiple_messages:
            for bufr_message in generate_bufr_message(decoder, s, continue_on_error=ns.continue_on_error,
                                                      file_path=filename, info_only=True):
                show_message_info(bufr_message)

        elif ns.count_only:
            count = 0
            for _ in generate_bufr_message(decoder, s, continue_on_error=ns.continue_on_error, info_only=True):
                count += 1
            print('{}: {}'.format(filename, count))

        else:
            bufr_message = decoder.process(s, file_path=filename, info_only=True)
            show_message_info(bufr_message)


def command_encode(ns):
    """
    Command to encode given JSON file from command line into BUFR file.
    """
    encoder = Encoder(definitions_dir=ns.definitions_directory,
                      tables_root_dir=ns.tables_root_directory,
                      compiled_template_cache_max=ns.compiled_template_cache_max,
                      master_table_version=ns.master_table_version)
    if ns.filename != '-':
        with open(ns.filename) as ins:
            s = ins.read()
    else:  # read from stdin, this is useful for piping
        s = sys.stdin.read()

    messages = {
        (True, True): 'Nested JSON',
        (True, False): 'Nested Text',
        (False, True): 'Flat JSON',
        (False, False): 'Flat Text',
    }
    try:
        if ns.json:
            data = json.loads(s)
            if ns.attributed:
                data = nested_json_to_flat_json(data)
        else:
            if ns.attributed:
                data = nested_text_to_flat_json(s)
            else:
                data = flat_text_to_flat_json(s)
    except (ValueError, SyntaxError):
        raise PyBufrKitError('Invalid input: Is it in {} format?'.format(
            messages[(ns.attributed, ns.json)]
        ))

    bufr_message = encoder.process(data, '<stdin>' if ns.filename else ns.filename,
                                   wire_template_data=False)
    if ns.output_filename:
        fmode = 'ab' if ns.append else 'wb'
        with open(ns.output_filename, fmode) as outs:
            if ns.preamble:
                outs.write(str.encode(ns.preamble, encoding='utf8'))
            outs.write(bufr_message.serialized_bytes)


def command_split(ns):
    """
    Command to split given files from command line into one file per
    BufrMessage.
    """
    decoder = Decoder(definitions_dir=ns.definitions_directory,
                      tables_root_dir=ns.tables_root_directory)

    for filename in ns.filenames:
        with open(filename, 'rb') as ins:
            s = ins.read()

        for idx, bufr_message in enumerate(
                generate_bufr_message(decoder, s, continue_on_error=ns.continue_on_error,
                                      file_path=filename, info_only=True)):
            new_filename = '{}.{}'.format(filename, idx)
            print(new_filename)
            with open(new_filename, 'wb') as outs:
                outs.write(bufr_message.serialized_bytes)


def command_lookup(ns):
    """
    Command to lookup the given descriptors from command line
    """
    table_group = TableGroupCacheManager.get_table_group(ns.tables_root_directory,
                                                         ns.master_table_number,
                                                         ns.originating_centre,
                                                         ns.originating_subcentre,
                                                         ns.master_table_version,
                                                         ns.local_table_version)
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


def command_compile(ns):
    """
    Command to compile the given descriptors.
    """
    from pybufrkit.templatecompiler import TemplateCompiler
    template_compiler = TemplateCompiler()

    if os.path.exists(ns.input):
        decoder = Decoder(definitions_dir=ns.definitions_directory,
                          tables_root_dir=ns.tables_root_directory)
        with open(ns.input, 'rb') as ins:
            bufr_message = decoder.process(ins.read(), file_path=ns.input, info_only=True)
            template, table_group = bufr_message.build_template(ns.tables_root_directory, normalize=1)
    else:
        table_group = TableGroupCacheManager.get_table_group(ns.tables_root_directory,
                                                             ns.master_table_number,
                                                             ns.originating_centre,
                                                             ns.originating_subcentre,
                                                             ns.master_table_version,
                                                             ns.local_table_version)
        descriptor_ids = [x.strip() for x in ns.input.split(',')]
        template = table_group.template_from_ids(*descriptor_ids)

    compiled_template = template_compiler.process(template, table_group)
    print(json.dumps(compiled_template.to_dict()))


def command_subset(ns):
    """
    Command to subset and save the given BUFR file.
    """
    decoder = Decoder(definitions_dir=ns.definitions_directory,
                      tables_root_dir=ns.tables_root_directory,
                      compiled_template_cache_max=ns.compiled_template_cache_max)
    encoder = Encoder(definitions_dir=ns.definitions_directory,
                      tables_root_dir=ns.tables_root_directory,
                      compiled_template_cache_max=ns.compiled_template_cache_max)

    subset_indices = [int(x) for x in ns.subset_indices.split(',')]
    with open(ns.filename, 'rb') as ins:
        s = ins.read()

    bufr_message = decoder.process(s, file_path=ns.filename, wire_template_data=False,
                                   ignore_value_expectation=ns.ignore_value_expectation)

    data = bufr_message.subset(subset_indices)
    nb = encoder.process(data, file_path=ns.output_filename, wire_template_data=False)

    with open(ns.output_filename, 'wb') as outs:
        outs.write(nb.serialized_bytes)


def command_query(ns):
    """
    Command to query given BUFR files.
    """
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
            from pybufrkit.dataquery import NodePathParser, DataQuerent
            querent = DataQuerent(NodePathParser())
            query_result = querent.query(bufr_message, ns.query_string)
            if ns.json:
                if ns.nested:
                    print(json.dumps(NestedJsonRenderer().render(query_result), **JSON_DUMPS_KWARGS))
                else:
                    print(json.dumps(FlatJsonRenderer().render(query_result), **JSON_DUMPS_KWARGS))
            else:
                print(filename)
                print(FlatTextRenderer().render(query_result))


def command_script(ns):
    """
    Command to execute script against given BUFR files.
    """
    from pybufrkit.script import ScriptRunner

    if ns.from_file:
        with open(ns.input) as ins:
            script_string = ins.read()
    else:
        if ns.input == '-':
            script_string = sys.stdin.read()
        else:
            script_string = ns.input

    script_runner = ScriptRunner(script_string,
                                 data_values_nest_level=ns.data_values_nest_level)

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
