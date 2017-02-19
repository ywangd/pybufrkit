"""
pybufrkit.script
~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import functools
import ast

from pybufrkit.dataquery import QueryResult
from pybufrkit.query import BufrMessageQuerent


def process_embedded_query_expr(input_string):
    """
    This function scans through the given script and identify any path/metadata
    expressions. For each expression found, an unique python variable name will
    be generated. The expression is then substituted by the variable name.

    :param str input_string: The input script
    :return:
    """
    keep = []
    state = ''
    idx_char = idx_var = 0
    substitutions = {}  # keyed by query expression

    query_expr = []
    while idx_char < len(input_string):
        c = input_string[idx_char]

        if state == '${':
            if c == '}':
                state = ''
                s = ''.join(query_expr).strip()
                query_expr = []
                if s not in substitutions:
                    varname = 'PBK_{}'.format(idx_var)
                    idx_var += 1
                    substitutions[s] = varname
                else:
                    varname = substitutions[s]
                keep.append(varname)
            else:
                query_expr.append(c)

        elif c == "'" or c == '"':
            if state == c:  # quoting pair found, pop it
                state = ''
            elif state == '':  # new quote begins
                state = c
            keep.append(c)

        elif c == '$' and state == '':  # an unquoted $
            if idx_char + 1 < len(input_string) and input_string[idx_char + 1] == '{':
                state = '${'
                idx_char += 1
            else:
                keep.append(c)

        else:
            keep.append(c)

        idx_char += 1

    return ''.join(keep), substitutions


DATA_VALUES_FLATTEN_0 = 0  # flatten to possible scalar
DATA_VALUES_FLATTEN_1 = 1  # flatten to a list with no nesting, this is the default
DATA_VALUES_FLATTEN_2 = 2  # flatten to a list nested with subsets
DATA_VALUES_FLATTEN_4 = 4  # no flatten at all


class ScriptRunner(object):
    def __init__(self, input_string,
                 data_values_flatten_level=DATA_VALUES_FLATTEN_1):
        self.code_string, self.substitutions = process_embedded_query_expr(input_string)

        self.pragma = {
            'data_values_flatten_level': DATA_VALUES_FLATTEN_1,
        }
        # Read pragma from inside the script
        self.process_pragma()

        # Pragma passed from function call has higher priority
        if data_values_flatten_level is not None:
            self.pragma['data_values_flatten_level'] = data_values_flatten_level

        self.code_object = compile(self.code_string, '', 'exec')

        self.metadata_only = True
        for query_str in self.substitutions.keys():
            if not query_str.startswith('%'):
                self.metadata_only = False
                break

        self.querent = BufrMessageQuerent()

    def run(self, bufr_message):
        variables = {
            varname: self.get_query_result(bufr_message, query_string)
            for query_string, varname in self.substitutions.items()
            }
        variables.update(
            {
                'PBK_BUFR_MESSAGE': bufr_message,
                'PBK_FILENAME': bufr_message.filename,
            }
        )

        exec (self.code_object, variables)

        return variables

    def get_query_result(self, bufr_message, query_expr):
        qr = self.querent.query(bufr_message, query_expr)
        if isinstance(qr, QueryResult):
            return self.flatten_data_values(qr)
        return qr

    def flatten_data_values(self, qr):
        data_values_flatten_level = self.pragma['data_values_flatten_level']

        if data_values_flatten_level == DATA_VALUES_FLATTEN_0:
            values = qr.all_values(flat=True)
            values = functools.reduce(lambda x, y: x + y, values, [])
            return values[0] if len(values) == 1 else values

        elif data_values_flatten_level == DATA_VALUES_FLATTEN_1:
            values = qr.all_values(flat=True)
            return functools.reduce(lambda x, y: x + y, values, [])

        elif data_values_flatten_level == DATA_VALUES_FLATTEN_2:
            return qr.all_values(flat=True)

        else:  # No flatten
            return qr.all_values()

    def process_pragma(self):
        for line in self.code_string.splitlines():
            if not line.startswith('#$'):
                return

            for assignment in line[3:].split(','):
                k, v = assignment.split('=')
                k = k.strip()
                if k in self.pragma:
                    self.pragma[k] = ast.literal_eval(v.strip())
