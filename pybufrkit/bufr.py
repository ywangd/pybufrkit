"""
The class represents the entire decoded BUFR message. Note it is different from
the `pybufrkit.bufrdata.BufrData` class, which is dedicated to the section 4
data with functions to provide a fully hierarchical view.
"""
from __future__ import absolute_import
import json

from .utils import format_decoded_data
from .bufrdata import BufrData


class Bufr(object):
    def __init__(self,
                 input_file_path,
                 table_group_info_string,
                 section0, section1, section2, section3, section4, section5,
                 n_subsets, is_compressed, template,
                 unexpanded_descriptors,
                 decoded_descriptors_all_subsets,
                 decoded_values_all_subsets,
                 bitmap_links_all_subsets):
        self.input_file_path = input_file_path
        self.table_group_info_string = table_group_info_string

        self.section0 = section0
        self.section1 = section1
        self.section2 = section2
        self.section3 = section3
        self.section4 = section4
        self.section5 = section5

        self.n_subsets = n_subsets
        self.is_compressed = is_compressed
        self.template = template
        self.unexpanded_descriptors = unexpanded_descriptors
        self.decoded_descriptors_all_subsets = decoded_descriptors_all_subsets
        self.decoded_values_all_subsets = decoded_values_all_subsets
        self.bitmap_links_all_subsets = bitmap_links_all_subsets

    def dumps(self, with_values=True):
        """
        Dump information of the BUFR object.

        :param bool with_values: Whether to dump the data in section 4.
        """
        ret = [self.input_file_path,
               self.table_group_info_string,
               '<<<<<< section 0 >>>>>>', str(self.section0), '',
               '<<<<<< section 1 >>>>>>', str(self.section1), '', ]

        if self.section2 is not None:
            ret.extend(['<<<<<< section 2 >>>>>>', str(self.section2), ''])
        ret.extend(['<<<<<< section 3 >>>>>>', str(self.section3), '',
                    '<<<<<< section 4 >>>>>>', str(self.section4), '', ])

        if with_values:
            ret.extend(format_decoded_data(self.n_subsets,
                                           self.decoded_descriptors_all_subsets,
                                           self.decoded_values_all_subsets,
                                           self.bitmap_links_all_subsets))

        return '\n'.join(ret)

    def jsons(self):
        """
        Produce a JSON string for the BUFR message that can be encoded back to
        the binary BUFR message.
        """
        data = [list(self.section0.values()),
                list(self.section1.values())]

        if self.section2 is not None:
            data.append(list(self.section2.values()))

        data.append(list(self.section3.values()))
        section4 = list(self.section4.values())
        section4.append(self.decoded_values_all_subsets)
        data.append(section4)
        data.append(list(self.section5.values()))

        return json.dumps(data, encoding='latin-1')

    def wire_data(self, template=None):
        """
        Wire the data section to create a hierarchical structure with attributes
        properly allocated to their corresponding values. The method generate a
        BufrData object that is dedicated to manage the data from section 4.

        :param template: A compatible template to use for the wiring. A compatible
                         template is one that expands to the same sequence of
                         descriptors as the original template.
        :rtype: pybufrkit.bufrdata.BufrData
        """
        return BufrData(template if template else self.template,
                        self.is_compressed,
                        self.decoded_descriptors_all_subsets,
                        self.decoded_values_all_subsets,
                        self.bitmap_links_all_subsets)
