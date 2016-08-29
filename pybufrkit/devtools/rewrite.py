import json
import os


def read_code_table(file_path):
    with open(file_path) as ins:
        lines = ins.readlines()

    ret = []
    for line in lines:
        fields = line.split(None, 2)
        ret.append([int(fields[0]), fields[2].strip() if len(fields) == 3 else ''])

    return ret


def read_code_tables(codtables_dir):
    table_file_list = [f for f in os.listdir(codtables_dir) if f.endswith('.table')]

    data = {}
    for table_file in table_file_list:
        data['{:06d}'.format(int(table_file.split('.')[0]))] = read_code_table(
            os.path.join(codtables_dir, table_file))

    return data


def rewrite_version(version_dir, output_dir):
    code_tables_data = read_code_tables(os.path.join(version_dir, '98/0/codetables'))
    with open(os.path.join(output_dir, 'code_and_flag.json'), 'w') as outs:
        json.dump(code_tables_data, outs, sort_keys=True)


def _rewrite(root_dir, output_dir):
    for version_dir in os.listdir(root_dir):
        this_output_dir = os.path.join(output_dir, version_dir)
        try:
            os.mkdir(this_output_dir)
        except OSError:
            pass
        rewrite_version(os.path.join(root_dir, version_dir), this_output_dir)


def rewrite(root_dir, output_dir='.'):
    for centre in ('local',):
        this_output_dir = os.path.join(output_dir, centre)
        try:
            os.mkdir(this_output_dir)
        except OSError:
            pass
        _rewrite(os.path.join(root_dir, centre), this_output_dir)

