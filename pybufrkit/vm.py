from __future__ import absolute_import
import os
import sys
import imp
from collections import OrderedDict
import functools
import uuid
import six.moves.builtins
from six.moves import range
from six.moves import zip

AST_UNPARSE_AVAILABLE = True
try:
    import astunparse
except ImportError:
    AST_UNPARSE_AVAILABLE = False

from .descriptors import (ElementDescriptor,
                          AssociatedDescriptor, MarkerDescriptor, SkippedLocalDescriptor)
from .tables import DEFAULT_TABLES_DIR
from .bpclpp import bpcl_pp
from .bpclcompiler import bpcl_compiler
from .templatecompiler import template_compiler
from .utils import BpclError
from .userland import collect_bpcl_builtins

if getattr(sys, 'frozen', False):  # for pyinstaller
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(__file__)

MAXIMUM_NUMBER_OF_CACHED_COMPILED_TEMPLATES = 100

DEFAULT_DEFINITIONS_DIR = os.path.join(BASE_DIR, 'definitions')
NUMERIC_MISSING_VALUES = [2 ** i - 1 for i in range(33)]

# Number of bits for represent number of bits used for difference
NBITS_FOR_NBITS_DIFF = 6

# Name for compiled template function
COMPILED_TEMPLATE_FILENAME = 'compiled_template.py'


def minmax(values):
    """
    Give a list of values, find out the minimum and maximum, ignore any Nones.
    """
    mn, mx = None, None
    for v in values:
        if v is not None:
            if mn is None or mn > v:
                mn = v
            if mx is None or mx < v:
                mx = v
    return mn, mx


class VM(object):
    """
    This class is an abstract superclass for Decoder and Encoder. By itself it
    cannot do anything. But it provides common operations for subclasses.

    :param str mode: The mode of the VM, can be either decoder or encoder
    :param definitions_dir: Where to find the BPCL definition files.
    :param definition_filename: The BPCL definition file name.
    :param tables_root_dir: Where to find the BUFR table files.
    :param cache_compiled_template: Whether to cache the compiled BUFR template.
           Note this is only a memory cache, not persistent to disk.
    :param compiled_template_dir: Where to load and save compiled templates.
    :param save_compiled_template: Whether to save compiled template to disk.
    """
    MODE_DECODER = 'decoder'
    MODE_ENCODER = 'encoder'

    def __init__(self,
                 mode,
                 definitions_dir=None,
                 definition_filename=None,
                 tables_root_dir=None,
                 cache_compiled_template=True,
                 compiled_template_dir=None,
                 save_compiled_template=False):

        if definitions_dir is None:
            definitions_dir = DEFAULT_DEFINITIONS_DIR

        if tables_root_dir is None:
            tables_root_dir = DEFAULT_TABLES_DIR

        self.cache_compiled_template = cache_compiled_template
        self.compiled_template_dir = compiled_template_dir
        self.compiled_templates = {}

        # TODO: externalize compiled template name
        if compiled_template_dir is not None:
            if not os.path.exists(compiled_template_dir):
                os.mkdir(compiled_template_dir)
            self.save_compiled_template = save_compiled_template
            for d in os.listdir(compiled_template_dir):
                m = imp.load_source('m', os.path.join(compiled_template_dir, d,
                                                      COMPILED_TEMPLATE_FILENAME))
                self.compiled_templates[m.__doc__] = m.process_template
        else:
            self.save_compiled_template = False

        # Collect more BPCL builtins
        self.bpcl_builtins = {}
        prefix = 'bpcl_{}_'.format(mode)
        for name, func in collect_bpcl_builtins().items():
            # remove leading bpcl_decode/encode_
            if name.startswith(prefix):
                self.bpcl_builtins[name[len(prefix):]] = functools.partial(func, self)
            else:  # remove the leading bpcl_ from the function name
                self.bpcl_builtins[name[5:]] = functools.partial(func, self)

        # Global namespace for BPCL source files. Ordered dictionary to
        # keep the order of variables from BPCL files.
        self.gns = OrderedDict(
            (
                ('__builtins__', dict(six.moves.builtins.__dict__)),
                ('_tables_root_dir', tables_root_dir),
                ('_definitions_dir', definitions_dir),
                ('_definition_file', definition_filename),
            )
        )
        # Now make all bpcl builtin functions available
        self.gns.update(self.bpcl_builtins)

        # Load definitions
        s = bpcl_pp.process(os.path.join(definitions_dir, definition_filename))
        self.boot_code = bpcl_compiler.compile(s)

    def get(self, item, default=None):
        """
        Enables a more dictionary like behaviour for accessing object attributes.

        :param str item: Name of the attribute to be accessed.
        :param default: Default return value if attribute does not exist
        """
        return getattr(self, item) if hasattr(self, item) else default

    def run(self):
        """
        Run bpcl code object, the bufr file should be given in the bpcl script.
        """
        # preflight is handled by the load user function
        exec(self.boot_code, self.gns)

    def _process_preflight(self):
        """
        Preflight before processing a file. It clears the gns namespace and
        remove any attribute variables that may be used by previous processing.
        """
        # Clear the global namespace. This is necessary when decoded is called multiple times.
        names_to_keep = list(self.bpcl_builtins.keys()) + ['__builtins__', '_tables_root_dir',
                                                     '_definitions_dir', '_definition_file']
        for name in list(self.gns.keys()):
            if name not in names_to_keep:
                self.gns.pop(name)

        for name in ('unexpanded_descriptors', 'table_group', 'template',  # these will be set by bpcl
                     'n_subsets', 'is_compressed',
                     'decoded_values_all_subsets',
                     'decoded_descriptors_all_subsets', 'decoded_descriptors',
                     'idx_subset', 'idx_value',
                     'refval_new',
                     'bitmap', 'bitmapped_descriptors', 'next_bitmapped_descriptor',
                     'back_reference_boundary', 'back_referenced_descriptors'):
            if hasattr(self, name):
                delattr(self, name)

    def _get_s4_process_func(self):
        """
        Retrieve the function to process the template or compile the template if
        a function does not already exist.

        :return: The function to process the template
        """
        compiled_template_key = '{} {}'.format(
            self.table_group,
            tuple(self.unexpanded_descriptors)
        )

        func = self.compiled_templates.get(compiled_template_key, None)

        if func is None:
            if not hasattr(self, 'template') or self.template is None:
                self.template = self.table_group.template_from_ids(
                    *self.unexpanded_descriptors)

            func, tree = template_compiler.compile(self.template, with_ast=True)

            if self.cache_compiled_template:
                if len(self.compiled_templates) >= MAXIMUM_NUMBER_OF_CACHED_COMPILED_TEMPLATES:
                    for _ in range(len(self.compiled_templates)
                                            + 1 - MAXIMUM_NUMBER_OF_CACHED_COMPILED_TEMPLATES):
                        self.compiled_templates.popitem()
                self.compiled_templates[compiled_template_key] = func

            if AST_UNPARSE_AVAILABLE and self.save_compiled_template:
                file_dir = os.path.join(
                    self.compiled_template_dir,
                    str(uuid.uuid3(uuid.NAMESPACE_OID, compiled_template_key))
                )
                if not os.path.isdir(file_dir):
                    os.mkdir(file_dir)
                with open(os.path.join(file_dir, COMPILED_TEMPLATE_FILENAME), 'wb') as outs:
                    outs.write('r"""{}"""\n'.format(compiled_template_key))
                    outs.write(astunparse.unparse(tree))

        return func

    def _process_s4_data_preflight(self, n_subsets, is_compressed):
        self.n_subsets = n_subsets
        self.is_compressed = is_compressed

        if is_compressed:
            # Compressed data has exactly the same decoded descriptors for each subset
            self.decoded_descriptors_all_subsets = [[]] * n_subsets
            self.decoded_descriptors = self.decoded_descriptors_all_subsets[0]
            self.bitmap_links_all_subsets = [{}] * n_subsets
            self.bitmap_links = self.bitmap_links_all_subsets[0]
        else:
            self.decoded_descriptors_all_subsets = [[] for _ in range(n_subsets)]
            self.bitmap_links_all_subsets = [{} for _ in range(n_subsets)]

        # 2 03 YYY (2 03 255 to conclude, not cancel)
        self.refval_new = {}
        # Bitmap related variables
        self.bitmap = None
        self.bitmapped_descriptors = None
        # Where to start count back for bitmap related descriptors
        self.back_reference_boundary = 0
        self.back_referenced_descriptors = None

    def get_descriptor_by_var_name(self, var_name, nbits=None):
        """
        Get Element, Operator, Associated, Skip local descriptors by the given
        name. This is needed for the template compilation.

        :param str var_name: Prefix + ID, prefix is one of E, A, S or O
        :param nbits: number of bits for the descriptor, only necessary for Associated
                      and Skip local descriptors
        """
        if var_name.startswith('E') or var_name.startswith('O'):  # Element or Operator descriptor
            # noinspection PyUnresolvedReferences
            return self.table_group.lookup(int(var_name[1:]))
        elif var_name.startswith('A'):  # Associated descriptor
            return AssociatedDescriptor(int(var_name[1:]), nbits)
        elif var_name.startswith('S'):  # Skip local descriptor
            return SkippedLocalDescriptor(int(var_name[1:]), nbits)
        else:
            raise BpclError('Descriptor variable name must start with either E, A, S or O')

    # noinspection PyAttributeOutsideInit
    def switch_subset_context(self, idx_subset):
        """
        This function is only useful for uncompressed data.
        """
        self.idx_subset = idx_subset
        # Reset refval_new to empty at start of each subset as anything defined
        # from previous subset should NOT affect this subset. Also we do not
        # care about what is defined in previous subset so we are not saving them.
        self.refval_new = {}
        self.decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
        self.decoded_values = self.decoded_values_all_subsets[idx_subset]
        self.bitmap_links = self.bitmap_links_all_subsets[idx_subset]
        # Index to value is only needed for encoder
        self.idx_value = 0

    def _get_delayed_replication_factor_value(self, idx):
        if self.is_compressed:
            self.assert_equal_values_of_index(idx)
            value = self.decoded_values_all_subsets[0][idx]
        else:
            value = self.decoded_values[idx]

        if value is None or value < 0:
            raise BpclError('Delayed replication factor must be >= 0: ({!r})'.format(value))

        return value

    def assert_equal_values_of_index(self, idx):
        """
        Assert that the values of the specified index are identical for all
        subsets. It is only used for compressed data. For an example, to ensure
        the delayed replication factors are the same for all subsets.
        """
        minv, maxv = minmax([values[idx] for values in self.decoded_values_all_subsets])
        assert minv == maxv, 'Values from all subsets are NOT identical'

    # noinspection PyAttributeOutsideInit
    def mark_back_reference_boundary(self):
        self.back_reference_boundary = len(self.decoded_descriptors)

    def _build_bitmapped_descriptors(self, bitmap):
        """
        Build the bitmapped descriptors based on the given bitmap. Also build
        the back referenced descriptors if it is not already defined.
        """
        # Second get all the back referenced descriptors if it does not already exist
        if not self.back_referenced_descriptors:
            self.back_referenced_descriptors = []
            for idx in range(self.back_reference_boundary - 1, -1, -1):
                descriptor = self.decoded_descriptors[idx]
                # The type has to be an exact match, not just isinstance
                if type(descriptor) is ElementDescriptor:
                    self.back_referenced_descriptors.insert(0, (idx, descriptor))
                    if len(self.back_referenced_descriptors) == len(bitmap):
                        break
        if len(self.back_referenced_descriptors) != len(bitmap):
            raise BpclError('Back referenced descriptors not matching defined Bitmap')

        # Lastly, get all the descriptors that has a corresponding Zero bit value
        self.bitmapped_descriptors = [
            (idx, d) for bit, (idx, d) in zip(
                bitmap,
                self.back_referenced_descriptors
            ) if bit == 0
            ]
        self.next_bitmapped_descriptor = functools.partial(next, iter(self.bitmapped_descriptors))

    # noinspection PyAttributeOutsideInit
    def recall_bitmap(self):
        g = iter(self.bitmapped_descriptors)
        self.next_bitmapped_descriptor = functools.partial(next, iter(self.bitmapped_descriptors))
        return self.bitmap

    # noinspection PyAttributeOutsideInit
    def cancel_bitmap(self):
        self.bitmap = None

    # noinspection PyAttributeOutsideInit
    def cancel_all_back_references(self):
        self.back_referenced_descriptors = None
        self.bitmap = None
        self.bitmapped_descriptors = None

    def add_bitmap_link(self):
        """
        Must be called before the descriptor is processed
        :return:
        """
        idx_descriptor, _ = self.next_bitmapped_descriptor()
        self.bitmap_links[len(self.decoded_descriptors)] = idx_descriptor

    def _process_bitmapped_descriptor(self,
                                      func_process_string,
                                      func_process_codeflag,
                                      func_process_numeric,
                                      func_process_numeric_with_new_refval,
                                      marker_id,
                                      nbytes_new,
                                      nbits_offset,
                                      scale_offset,
                                      nbits_increment,
                                      scale_increment,
                                      refval_factor):

        """
        A generic method for processing bitmapped descriptors. It is wrapped by
        providing different funcs to handle encoding and decoding for
        uncompressed and compressed data.
        """

        idx_descriptor, bitmapped_descriptor = self.next_bitmapped_descriptor()
        self.bitmap_links[len(self.decoded_descriptors)] = idx_descriptor

        # difference statistical values marker has different refval and nbits values
        if marker_id == 225255:
            bitmapped_descriptor = MarkerDescriptor.from_element_descriptor(
                bitmapped_descriptor,
                marker_id,
                refval=-2 ** bitmapped_descriptor.nbits,
                nbits=bitmapped_descriptor.nbits + 1,
            )
        else:
            bitmapped_descriptor = MarkerDescriptor.from_element_descriptor(
                bitmapped_descriptor,
                marker_id,
            )

        if bitmapped_descriptor.unit == 'CCITT IA5':
            nbytes = nbytes_new if nbytes_new else bitmapped_descriptor.nbits // 8
            func_process_string(bitmapped_descriptor, nbytes)

        elif bitmapped_descriptor.unit in ('FLAG TABLE', 'CODE TABLE'):
            func_process_codeflag(bitmapped_descriptor, bitmapped_descriptor.nbits)

        else:  # numeric
            nbits = bitmapped_descriptor.nbits + nbits_offset + nbits_increment
            scale = bitmapped_descriptor.scale + scale_offset + scale_increment
            scale_powered = 1.0 * 10 ** scale

            if bitmapped_descriptor.id not in self.refval_new:
                refval = bitmapped_descriptor.refval * refval_factor
                func_process_numeric(bitmapped_descriptor, nbits, scale_powered, refval)
            else:
                func_process_numeric_with_new_refval(
                    bitmapped_descriptor, nbits, scale_powered, refval_factor)
