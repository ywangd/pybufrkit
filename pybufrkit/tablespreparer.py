from __future__ import absolute_import, print_function

import argparse
import csv
import io
import json
import os
import zipfile

try:  # py3
    from urllib.request import urlopen
except ImportError:  # py2
    from urllib2 import urlopen

__all__ = ['prepare_wmo_tables']


def prepare_wmo_tables(version, tag):
    data = download_wmo_bufr_tables_release(version, tag)
    tables = convert_tables_from_zip(version, data)
    write_tables(version, tables, '.')


def download_wmo_bufr_tables_release(version, tag):
    """
    Download WMO BUFR4 tables release of the specified version from its GitHub repo
    """
    download_url = 'https://github.com/wmo-im/BUFR4/archive/refs/tags/v{}.zip'.format(tag)
    print('Downloading WMO tables version {} from {}'.format(version, download_url))
    ins = urlopen(download_url)
    return ins.read()


def convert_tables_from_zip(version, data):
    print('Converting tables')
    zf = zipfile.ZipFile(io.BytesIO(data), 'r')
    table_b = {}
    table_d = {}
    table_code_and_flag = {}
    for fileinfo in zf.infolist():
        if fileinfo.filename.startswith('BUFR4-{}{}BUFRCREX_TableB_en_'.format(version, os.path.sep)):
            print('Table B: ' + fileinfo.filename)
            table_b.update(process_table_b(zf.read(fileinfo).decode('utf-8')))
        elif fileinfo.filename.startswith('BUFR4-{}{}BUFR_TableD_en_'.format(version, os.path.sep)):
            table_d.update(process_table_d(zf.read(fileinfo).decode('utf-8')))
        elif fileinfo.filename.startswith('BUFR4-{}{}BUFRCREX_CodeFlag_en_'.format(version, os.path.sep)):
            table_code_and_flag.update(process_table_code_and_flag(zf.read(fileinfo).decode('utf-8')))
        # TODO: MetaA and MetaC

    return {
        'b': table_b,
        'd': table_d,
        'code_and_flag': table_code_and_flag,
    }


def write_tables(version, tables, output_dir):
    base_dir = os.path.join(output_dir, '{}'.format(version))
    print('Saving tables inside folder: {}'.format(base_dir))
    os.makedirs(base_dir)
    with open(os.path.join(base_dir, 'TableB.json'), 'w') as outs:
        json.dump(tables['b'], outs, sort_keys=True)
    with open(os.path.join(base_dir, 'TableD.json'), 'w') as outs:
        json.dump(tables['d'], outs, sort_keys=True)
    with open(os.path.join(base_dir, 'code_and_flag.json'), 'w') as outs:
        json.dump(tables['code_and_flag'], outs, sort_keys=True)
    print('Done')


def process_table_b(content):
    lines = csv.reader(io.StringIO(content), quoting=csv.QUOTE_MINIMAL)
    next(lines)  # skip header
    # WMO output is inconsistent and can have extra comma after 3rd column (name)
    offset = 0
    d = {}
    for line in lines:
        crex_scale = 0 if line[9 + offset] == '' else int(line[9 + offset])
        crex_data_width = 0 if line[10 + offset] == '' else int(line[10 + offset])
        d[line[2]] = [line[3], line[4 + offset], int(line[5 + offset]), int(line[6 + offset]), int(line[7 + offset]), line[8 + offset], crex_scale, crex_data_width]
    return d


def process_table_d(content):
    lines = csv.reader(io.StringIO(content), quoting=csv.QUOTE_MINIMAL)
    next(lines)  # skip header
    d = {}
    for line in lines:
        entry = d.setdefault(line[2], [line[3], []])
        entry[1].append(line[5])
    return d


def process_table_code_and_flag(content):
    lines = csv.reader(io.StringIO(content), quoting=csv.QUOTE_MINIMAL)
    next(lines)  # skip header
    d = {}
    for line in lines:
        entry = d.setdefault(line[0], [])
        entry.append([line[2], line[3]])
    return d


if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        description='Download BUFR tables published by WMO amd convert them to PyBufrKit format')
    ap.add_argument('version', type=int, help='the table version')
    ap.add_argument('--tag', help='the release tag (same as the table version if not specified')
    ns = ap.parse_args()
    prepare_wmo_tables(ns.version, str(ns.version) if ns.tag is None else ns.tag)
