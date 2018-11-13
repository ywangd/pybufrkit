"""
pybufrkit.utils
~~~~~~~~~~~~~~~
"""
from __future__ import absolute_import
from __future__ import print_function

import ast
import json
import six


def flatten_list(values):
    """
    Flatten a list so everything is in a list without nesting
    .
    :param values:
    :return:
    """
    flat_values = []
    for entry in values:
        if isinstance(entry, list):
            flat_values += flatten_list(entry)
        else:
            flat_values.append(entry)
    return flat_values


# Encode bytes as string for Python 3
class EntityEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, six.binary_type):
            return o.decode(encoding='latin-1')

        return json.JSONEncoder.default(self, o)


JSON_DUMPS_KWARGS = {'encoding': 'latin-1'} if six.PY2 else {'cls': EntityEncoder}


def fixed_width_repr_of_int(value, width, pad_left=True):
    """
    Format the given integer and ensure the result string is of the given
    width. The string will be padded space on the left if the number is 
    small or replaced as a string of asterisks if the number is too big.
    
    :param int value: An integer number to format 
    :param int width: The result string must have the exact width 
    :return: A string representation of the given integer. 
    """
    ret = '{:{pad_dir}{width}d}'.format(value, pad_dir='>' if pad_left else '>', width=width)
    return '*' * width if len(ret) > width else ret


def nested_json_to_flat_json(nested_json_data):
    """
    Converted the nested JSON output to the flat JSON output. This is
    useful as Encoder only works with flat JSON.

    :param nested_json_data: The nested JSON object
    :return: Flat JSON object.
    """
    flat_json_data = []
    for nested_section_data in nested_json_data:
        flat_section_data = []
        for parameter_data in nested_section_data:
            if parameter_data['name'] == 'template_data':
                flat_section_data.append(
                    template_data_nested_json_to_flat_json(parameter_data['value'])
                )
            else:
                flat_section_data.append(parameter_data['value'])
        flat_json_data.append(flat_section_data)

    return flat_json_data


def template_data_nested_json_to_flat_json(template_data_value):
    """
    Helper function to convert nested JSON of template data to flat JSON.
    """

    def process_value_parameter(data, parameter):
        # Just process the first layer of attributes. No value is needed
        # from any nested attributes as they must be virtual
        for attr in parameter.get('attributes', []):
            if 'virtual' not in attr:
                data.append(attr['value'])
        # Associated Field value appears before the value of the owner node
        data.append(parameter['value'])

    def process_members(data, members):
        for parameter in members:
            if 'value' in parameter:
                process_value_parameter(data, parameter)

            else:
                if 'factor' in parameter:
                    process_value_parameter(data, parameter['factor'])

                if 'members' in parameter:
                    if parameter['id'].startswith('1'):  # Replication
                        for members in parameter['members']:
                            process_members(data, members)
                    else:
                        process_members(data, parameter['members'])

    data_all_subsets = []
    for nested_subset_data in template_data_value:
        flat_subset_data = []
        process_members(flat_subset_data, nested_subset_data)
        data_all_subsets.append(flat_subset_data)

    return data_all_subsets


TEXT_SECTION_HEADER = '<<<<<<'
TEXT_SUBSET_HEADER = '######'


def section_text_to_flat_json(lines, idxline, func_subsets_text_to_flat_json):
    """
    Convert a section from text output to a section of flat JSON.
    """
    section_data = []
    idxline += 1  # skip the section header
    while idxline < len(lines):
        line = lines[idxline]
        if line.startswith(TEXT_SECTION_HEADER):
            break
        if line.startswith(TEXT_SUBSET_HEADER):
            idxline, data_all_subsets = func_subsets_text_to_flat_json(lines, idxline)
            section_data.append(data_all_subsets)
            continue
        parameter_name, value = line.split(' = ')
        section_data.append(ast.literal_eval(value))
        idxline += 1

    return idxline, section_data


def flat_text_to_flat_json(flat_text):
    """
    Convert the flat Text output to the flat JSON output format
    
    :param str flat_text: The flat text output
    """
    flat_json = []
    # Skip the first line of table group key info
    lines = flat_text.splitlines()[1:]
    idxline = 0
    while idxline < len(lines):
        idxline, section_data = section_text_to_flat_json(lines, idxline, subsets_flat_text_to_flat_json)
        flat_json.append(section_data)

    return flat_json


def subsets_flat_text_to_flat_json(lines, idxline):
    """
    Convert all subsets data from flat text output to all subsets data of flat JSON.
    """
    data_all_subsets = []
    while True:
        line = lines[idxline]
        if line.startswith(TEXT_SECTION_HEADER):
            break
        if line.startswith(TEXT_SUBSET_HEADER):
            data_all_subsets.append([])
            idxline += 1
            continue
        value = ast.literal_eval(line[81:].strip())
        if isinstance(value, tuple):
            value = value[0]
        data_all_subsets[-1].append(value)
        idxline += 1

    return idxline, data_all_subsets


def nested_text_to_flat_json(nested_text):
    """
    Convert string in nested text format to a flat JSON object.
    :param str nested_text: The nested text output 
    :return: A flat JSON object
    """
    flat_json = []
    # Skip the first line of table group key info
    lines = nested_text.splitlines()[1:]
    idxline = 0
    while idxline < len(lines):
        idxline, section_data = section_text_to_flat_json(lines, idxline, subsets_nested_text_to_flat_json)
        flat_json.append(section_data)

    return flat_json


def subsets_nested_text_to_flat_json(lines, idxline):
    """
    Convert all subsets data from nested text format to flat JSON format.
    """
    data_all_subsets = []
    while True:
        line = lines[idxline].strip()
        if line.startswith(TEXT_SECTION_HEADER):
            break
        if line.startswith(TEXT_SUBSET_HEADER):
            data_all_subsets.append([])
            idxline += 1
            continue
        # Skip comments (replication header), attributed fields and Sequence descriptors
        if line.startswith('#') \
                or (line.startswith('->') and not line.startswith('-> A')) \
                or line.startswith('3'):
            idxline += 1
            continue

        # Entries with no values, e.g. 204YYY, 1XXYYY
        if ' ' not in line:
            idxline += 1
            continue

        line_trailing_char = line[-1]
        if line_trailing_char not in ('"', "'"):
            value = ast.literal_eval(line.rsplit(' ', 1)[1])
        else:
            string_left_bound = (' ' if six.PY2 else ' b') + line_trailing_char
            idxval = line.rfind(string_left_bound, 0, len(line) - 1)
            value = ast.literal_eval(line[idxval + 1:])

        # Insert associated field before the owner field
        if line.startswith('-> A'):
            data_all_subsets[-1].insert(-1, value)
        else:
            data_all_subsets[-1].append(value)
        idxline += 1
    return idxline, data_all_subsets


def generate_quiet(iterable, next_val):
    """
    Iterate, returning if the generator function raises StopIteration.

    https://www.python.org/dev/peps/pep-0479/
    """
    for _ in iterable:
        try:
            yield next_val()
        except StopIteration:
            return
