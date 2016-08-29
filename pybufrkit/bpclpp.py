from __future__ import absolute_import
import os
import ast

__all__ = ['bpcl_pp', 'peek_mode']


def peek_mode(file_path):
    """
    Read the first line of the input file and find out the mode it should runs in.
    """
    mode = 'decoder'
    with open(file_path) as ins:
        line = ins.readline()
        if line.startswith('#!'):
            mode = line.split()[0][2:].lower()

    # Default mode is decode for any invalid input
    if mode not in ('decoder', 'encoder'):
        mode = 'decoder'

    return mode


class PreProcessor(object):
    def __init__(self):
        self.file_abspath_stack = []

    def process(self, file_path):
        lines = self._process(file_path)
        return ''.join(lines)

    def _process(self, file_path):

        if os.path.isabs(file_path):
            file_abspath = file_path
        else:
            file_abspath = os.path.abspath(file_path)

        self.file_abspath_stack.append(file_abspath)
        file_dir = os.path.dirname(file_abspath)

        with open(file_abspath) as ins:
            text = ins.read()

        lines = text.splitlines(True)

        line_continuation = False
        ret = []
        for line in lines:
            if line_continuation:
                prev_line = ret.pop()
                line = prev_line + ' {}'.format(line)
                line_continuation = False

            if line.rstrip().endswith('\\'):  # line continuation
                ret.append(line.rstrip()[:-1])
                line_continuation = True
                continue

            if line.lstrip().startswith('@include'):
                try:
                    sub_file_path = ast.literal_eval(line[8:].strip())
                    # TODO: Envvars expansion necessary?
                    sub_file_path = os.path.expandvars(sub_file_path)
                except SyntaxError:
                    raise RuntimeError('Invalid include: {}'.format(line))

                if os.path.isabs(sub_file_path):
                    sub_file_abspath = sub_file_path
                else:
                    sub_file_abspath = os.path.abspath(os.path.join(file_dir, sub_file_path))

                if sub_file_abspath in self.file_abspath_stack:
                    raise RuntimeError('Circular inclusion detected: {}'.format(sub_file_path))

                ret.extend(self._process(sub_file_abspath))
                # Always add an extra blank line in case that the included file does not have an ending NEWLINE
                ret.append('\n')

            else:
                ret.append(line)

        self.file_abspath_stack.pop()
        return ret


bpcl_pp = PreProcessor()
