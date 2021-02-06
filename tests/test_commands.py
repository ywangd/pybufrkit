from __future__ import absolute_import
from __future__ import print_function
import os
import tempfile
import shutil

from pybufrkit.commands import command_encode

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


class NS(object):

    def __init__(self, m):
        self._m = m

    def __getattr__(self, item):
        return self.__dict__['_m'].get(item, None)


def test_command_encode_with_append():
    input_file = os.path.join(DATA_DIR, 'IUSK73_AMMC_182300.json')
    output_dir = tempfile.mkdtemp()
    output_file = os.path.join(output_dir, 'out.bufr')
    with open(output_file, 'wb') as outs:
        outs.write(b'IOBI01 SBBR 011100\r\r\n')
    ns = NS(
        {'filename': input_file, 'output_filename': output_file, 'append': True, 'json': True, ', attributed': False})

    command_encode(ns)
    with open(output_file, 'rb') as ins:
        assert ins.read().startswith(b'IOBI01 SBBR 011100\r\r\n')

    shutil.rmtree(output_dir, ignore_errors=True)


def test_command_encode_with_preamble():
    input_file = os.path.join(DATA_DIR, 'IUSK73_AMMC_182300.json')
    output_dir = tempfile.mkdtemp()
    output_file = os.path.join(output_dir, 'out.bufr')
    ns = NS(
        {'filename': input_file, 'output_filename': output_file, 'append': False, 'json': True, ', attributed': False,
         'preamble': 'IOBI01 SBBR 011100\r\r\n'})

    command_encode(ns)
    with open(output_file, 'rb') as ins:
        assert ins.read().startswith(b'IOBI01 SBBR 011100\r\r\n')

    shutil.rmtree(output_dir, ignore_errors=True)
