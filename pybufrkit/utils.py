from __future__ import absolute_import
from __future__ import print_function


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
