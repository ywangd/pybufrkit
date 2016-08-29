import json
import re
from collections import OrderedDict


def read_table_B(file_path):
    with open(file_path) as ins:
        lines = ins.readlines()

    ret = OrderedDict()
    for line in lines:
        if line.startswith('#'):
            continue

        fields = line.strip().split('|')
        id_ = fields[0]
        abbr = fields[1]
        name = fields[3]
        unit = fields[4]
        scale = int(fields[5])
        refval = int(fields[6])
        nbits = int(fields[7])
        crex_unit = fields[8]
        crex_scale = int(fields[9])
        crex_nbits = int(fields[10])

        ret[id_] = [name, unit, scale, refval, nbits, crex_unit, crex_scale, crex_nbits]

    return ret


def write_table_B_json(a):
    with open('TableB.json', 'w') as outs:
        json.dump(a, outs)


def read_table_D(file_path):
    names = {}
    with open('TableD_description.txt') as ins:
        for line in ins.readlines():
            names[line[0:6]] = line[7:].strip()

    lines = []
    with open(file_path) as ins:
        for line in ins.readlines():
            if line.startswith('"'):
                lines.append(line)
            else:
                lines[-1] = lines[-1].rstrip() + ' ' + line.lstrip()

    p = re.compile(r'"([0-9]+)" *= *\[(.*)\]')

    ret = OrderedDict()
    for line in lines:
        m = p.match(line)

        d, line = m.groups()
        fields = line.replace(' ', '').split(',')

        ret[d] = [names.get(d, ''), fields]

    return ret


def write_table_D_json(a):
    with open('TableD.json', 'w') as outs:
        json.dump(a, outs)


def read_code_and_flag(file_path):
    with open(file_path) as ins:
        lines = ins.readlines()

    ret = OrderedDict()
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        id_, nentries, code_or_bit, nrows, meaning = \
            line[0:6], int(line[7: 11]), \
            int(line[12: 20]), int(line[21: 23]), line[24:].strip()

        ret[id_] = []

        idx += 1
        for irow in xrange(nrows - 1):  # complete the meaning
            meaning += ' ' + lines[idx].strip()
            idx += 1
        for ientry in xrange(nentries - 1):  # read all entries of a member
            line = lines[idx]
            code_or_bit, nrows, meaning = int(line[12: 20]), int(line[21: 23]), line[24:].strip()
            idx += 1
            for irow in xrange(nrows - 1):  # complete the meaning
                meaning += ' ' + lines[idx].strip()
                idx += 1

            ret[id_].append((code_or_bit, meaning))

    return ret


def write_code_and_flag_json():
    a = read_code_and_flag('code_and_flag.table')
    with open('Table_code_and_flag.json', 'w') as outs:
        json.dump(a, outs)


if __name__ == '__main__':
    write_table_B_json(read_table_B('element.table'))
    write_table_D_json(read_table_D('sequence.def'))
    pass
