"""
Compile a BUFR template to a python script that processes files using the
template. The compiled template can be used for both decoding and decoding for
both uncompressed and compressed data.
"""
from __future__ import absolute_import
import numbers
import ast
import contextlib
from collections import namedtuple, OrderedDict
from six import PY3
from six.moves import range

from .descriptors import (ElementDescriptor, FixedReplicationDescriptor,
                          DelayedReplicationDescriptor, OperatorDescriptor,
                          SequenceDescriptor, AssociatedDescriptor,
                          SkippedLocalDescriptor)
from .utils import BpclError

__all__ = ['template_compiler']

# Bitmap definition stage
BITMAP_NA = 0  # e.g. not in a bitmap definition block
BITMAP_INDICATOR = 1  # e.g. 222000, 223000
BITMAP_WAITING_FOR_BIT = 4
BITMAP_BIT_COUNTING = 5  # processing 031031

# STATUS of QA info follows, 222000
QA_INFO_NA = 0  # no in QA info follows range
QA_INFO_WAITING = 1  # after seeing 222000
QA_INFO_PROCESSING = 2  # after seeing the first class 33 descriptor

# Nbits, Scale and Reference value Modifiers for 207 YYY
NsrMod = namedtuple('NsrMod', ['nbits_increment', 'scale_increment', 'refval_factor'])


def make_var_name_for_descriptor(descriptor):
    """
    Make a variable name for the given descriptor
    """
    descriptor_type = type(descriptor)
    if descriptor_type in (ElementDescriptor, AssociatedDescriptor, SkippedLocalDescriptor,
                           OperatorDescriptor):
        # The prefix is the first letter of the descriptor's class
        # So E for ElementDescriptor, A for AssociatedDescriptor, S for SkippedLocalDescriptor
        # and O for OperatorDescriptor
        prefix = descriptor_type.__name__[0]
    else:
        raise BpclError('Var name is not needed for {} type descriptor'.format(descriptor_type))

    return '{}{:06d}'.format(prefix, descriptor.id)


@contextlib.contextmanager
def replication_loop(self, n_repeats=None):
    """
    :param TemplateCompiler self:
    :param n_repeats: Number of repeats or None for delayed replication
    """
    n0 = len(self.process_worker_stmts)
    yield
    body = [self.process_worker_stmts.pop() for _ in range(len(self.process_worker_stmts) - n0)][::-1]
    node = ast.For(
        target=ast.Name(id='idx_replication', ctx=ast.Store()),
        iter=ast.Call(
            func=ast.Name(id='range', ctx=ast.Load()),
            args=[
                ast.Num(n=n_repeats) if n_repeats else ast.Call(
                    func=ast.Name(id='get_delayed_replication_factor_value', ctx=ast.Load()),
                    args=[], keywords=[], starargs=None, kwargs=None
                )
            ],
            keywords=[], starargs=None, kwargs=None
        ),
        body=body,
        orelse=[]
    )
    self.process_worker_stmts.append(node)


# noinspection PyAttributeOutsideInit,PyTypeChecker
class TemplateCompiler(object):
    def compile(self, template, with_ast=True):
        """
        Main method of the class.

        :param pybufrkit.descriptors.BufrTemplate template:
        :param bool with_ast: whether to return AST tree
        :return: The function to process the template and optionally the AST tree for
                 defining the function.
        """
        self.compile_preflight()

        self.compile_members(template.members)

        if PY3:
            arguments = ast.arguments(
                args=[], vararg=None,
                kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[])
            worker_func = ast.FunctionDef(
                name='worker_func',
                args=arguments,
                body=(self.process_worker_stmts if self.process_worker_stmts else
                      [ast.Pass()]),  # in case data is empty
                decorator_list=[],
                returns=None
            )

        else:
            arguments = ast.arguments(
                args=[], vararg=None, kwarg=None, defaults=[])
            worker_func = ast.FunctionDef(
                name='worker_func',
                args=arguments,
                body=(self.process_worker_stmts if self.process_worker_stmts else
                      [ast.Pass()]),  # in case data is empty
                decorator_list=[]
            )

        process_stmt_list = [
            # The worker function for both compressed and uncompressed
            worker_func,
            # User of the worker function that adds a subset loop for uncompressed data
            ast.If(
                test=ast.Name(id='is_compressed', ctx=ast.Load()),
                body=[ast.Expr(value=ast.Call(func=ast.Name(id='worker_func', ctx=ast.Load()),
                                              args=[], keywords=[], starargs=None, kwargs=None))],
                orelse=[
                    ast.For(target=ast.Name(id='idx_subset', ctx=ast.Store()),
                            iter=ast.Call(
                                func=ast.Name(id='range', ctx=ast.Load()),
                                args=[ast.Name(id='n_subsets', ctx=ast.Load())],
                                keywords=[], starargs=None, kwargs=None),
                            body=[
                                ast.Expr(value=ast.Call(
                                    func=ast.Name(id='switch_subset_context', ctx=ast.Load()),
                                    args=[ast.Name(id='idx_subset', ctx=ast.Load())],
                                    keywords=[], starargs=None, kwargs=None)),
                                ast.Expr(value=ast.Call(
                                    func=ast.Name(id='worker_func', ctx=ast.Load()),
                                    args=[], keywords=[], starargs=None, kwargs=None))],
                            orelse=[])
                ]
            )
        ]

        # TODO: embed vars in the code object, possible??
        # Add conditional definitions of descriptor variables
        process_stmt_list = self.compile_descriptor_vars() + process_stmt_list

        tree = ast.fix_missing_locations(make_funcdef(process_stmt_list))
        co = compile(tree, '<template>', mode='exec')

        # Get the function by running the function definition
        exec (co)
        process_func = locals()['process_template']
        return (process_func, tree) if with_ast else process_func

    def compile_preflight(self):
        """
        Initialize variables before compilation.
        """
        self.process_worker_stmts = []
        self.process_descriptor_vars = OrderedDict()

        self.nbits_offset = 0  # 201 YYY
        self.scale_offset = 0  # 202 YYY

        # 203 YYY
        self.nbits_new_refval = 0
        self.refval_new_ids = []

        self.nbits_associated_list = []  # 204 YYY
        self.nbits_skipped_local_descriptor = 0  # 206

        self.nsr_modifier = NsrMod(
            nbits_increment=0, scale_increment=0, refval_factor=1
        )  # 207 YYY
        self.nbytes_new = 0  # 208 YYY

        self.data_not_present_count = 0  # 221
        self.status_qa_info_follows = QA_INFO_NA  # 222

        # bitmap definition
        self.bitmap_definition_state = BITMAP_NA
        self.most_recent_bitmap_is_for_reuse = False

    def add_process_worker_call(self, func_name, *args):
        arguments = []
        for arg in args:
            if isinstance(arg, bool):
                arguments.append(ast.Name(id=str(arg), ctx=ast.Load()))
            elif isinstance(arg, numbers.Number):
                arguments.append(ast.Num(n=arg))
            elif isinstance(arg, ast.AST):
                arguments.append(arg)
            else:  # descriptors
                name = make_var_name_for_descriptor(arg)
                if name not in self.process_descriptor_vars:
                    self.process_descriptor_vars[name] = arg
                arguments.append(ast.Name(id=name, ctx=ast.Load()))

        node = ast.Expr(
            value=ast.Call(
                func=ast.Name(id=func_name, ctx=ast.Load()),
                args=arguments, keywords=[], starargs=None, kwargs=None
            )
        )

        self.process_worker_stmts.append(node)

    def compile_init_bitmap_bit_count(self):
        self.process_worker_stmts.append(
            ast.Assign(targets=[ast.Name(id='n_031031', ctx=ast.Store())],
                       value=ast.Num(n=0))
        )

    def compile_increase_bitmap_bit_count(self):
        self.process_worker_stmts.append(
            ast.AugAssign(target=ast.Name(id='n_031031', ctx=ast.Store()),
                          op=ast.Add(), value=ast.Num(n=1))
        )

    def compile_descriptor_vars(self):
        stmts = []
        for name, descriptor in self.process_descriptor_vars.items():
            args = [ast.Str(s=name)]
            if isinstance(descriptor, (AssociatedDescriptor, SkippedLocalDescriptor)):
                args.append(ast.Num(n=descriptor.nbits))

            node = ast.Assign(
                targets=[ast.Name(id=name, ctx=ast.Store())],
                value=ast.Call(
                    func=ast.Name(id='get_descriptor_by_var_name', ctx=ast.Load()),
                    args=args,
                    keywords=[], starargs=None, kwargs=None
                )
            )
            stmts.append(node)

        return stmts

    def compile_define_new_refval(self, descriptor):
        if descriptor.unit == 'CCITT IA5':
            raise BpclError('Trying to define new reference value for string type descriptor')
        # TODO: new descriptor type for defining new refval?
        self.add_process_worker_call('process_new_refval', descriptor, self.nbits_new_refval)

    def compile_associated_field(self, descriptor):
        nbits_associated = sum(self.nbits_associated_list)
        self.add_process_worker_call('process_codeflag',
                                     AssociatedDescriptor(descriptor.id, nbits_associated),
                                     nbits_associated)

    def compile_element_descriptor(self, descriptor):
        X = descriptor.X

        # Read associated field if exists
        # Page 79 of layer 3 Guide, operators do not apply to class 31 element descriptor
        if self.nbits_associated_list and X != 31:
            self.compile_associated_field(descriptor)

        # Handle class 33 codes for QA information follows 222000 operator
        if X == 33:
            if self.status_qa_info_follows == QA_INFO_WAITING:
                self.status_qa_info_follows = QA_INFO_PROCESSING
            # Add the link between the QA info and its corresponding descriptor
            if self.status_qa_info_follows == QA_INFO_PROCESSING:
                self.add_process_worker_call('add_bitmap_link')
        else:
            if self.status_qa_info_follows == QA_INFO_PROCESSING:
                self.status_qa_info_follows = QA_INFO_NA

        # Now we can process the element normally
        if descriptor.unit == 'CCITT IA5':
            nbytes = self.nbytes_new if self.nbytes_new else descriptor.nbits // 8
            self.add_process_worker_call('process_string',
                                         descriptor, nbytes)

        elif descriptor.unit in ('FLAG TABLE', 'CODE TABLE'):
            self.add_process_worker_call('process_codeflag',
                                         descriptor, descriptor.nbits)

        else:
            nbits = descriptor.nbits + \
                    self.nbits_offset + \
                    self.nsr_modifier.nbits_increment
            scale = descriptor.scale + \
                    self.scale_offset + \
                    self.nsr_modifier.scale_increment
            scale_powered = 1.0 * 10 ** scale

            if descriptor.id not in self.refval_new_ids:  # no new refval is defined for this descriptor
                refval = descriptor.refval * self.nsr_modifier.refval_factor
                self.add_process_worker_call('process_numeric',
                                             descriptor, nbits, scale_powered, refval)

            else:  # a new refval is defined for the descriptor, it must be retrieved at runtime
                self.add_process_worker_call('process_numeric_with_new_refval',
                                             descriptor, nbits, scale_powered,
                                             self.nsr_modifier.refval_factor)

    def compile_fixed_replication_descriptor(self, descriptor):
        """
        :param FixedReplicationDescriptor descriptor:
        """
        with replication_loop(self, descriptor.n_repeats):
            self.compile_members(descriptor.members)

    def compile_delayed_replication_descriptor(self, descriptor):
        """
        :param DelayedReplicationDescriptor descriptor:
        """
        # TODO: delayed repetition descriptor 031011, 031012
        if descriptor.id in (31011, 31012):
            raise NotImplementedError('delayed repetition descriptor')

        self.compile_element_descriptor(descriptor.factor)
        with replication_loop(self):
            self.compile_members(descriptor.members)

    def compile_sequence_descriptor(self, descriptor):
        self.compile_members(descriptor.members)

    def compile_operator_descriptor(self, descriptor):
        """
        :param OperatorDescriptor descriptor:
        :return:
        """
        operator_code, operand_value = descriptor.operator_code, descriptor.operand_value

        if operator_code == 201:  # nbits offset
            self.nbits_offset = (operand_value - 128) if operand_value else 0

        elif operator_code == 202:  # scale offset
            self.scale_offset = (operand_value - 128) if operand_value else 0

        elif operator_code == 203:  # new reference value
            if operand_value == 0:
                self.refval_new_ids = []
                self.nbits_new_refval = operand_value
            else:  # 255
                self.nbits_new_refval = 0

        elif operator_code == 204:  # associated field
            if operand_value == 0:
                self.nbits_associated_list.pop()
            else:
                self.nbits_associated_list.append(operand_value)

        elif operator_code == 205:  # read string of YYY bytes
            # TODO: Need take care of associated field?
            # TODO: this is not affected by nbytes_new 208 YYY
            self.add_process_worker_call('process_string', descriptor, operand_value)

        elif operator_code == 206:  # skip local descriptor of YYY bits
            self.nbits_skipped_local_descriptor = operand_value

        elif operator_code == 207:  # increase nbits, scale, refval
            if operand_value == 0:
                self.nsr_modifier = NsrMod(
                    nbits_increment=0, scale_increment=0, refval_factor=1
                )
            else:
                self.nsr_modifier = NsrMod(
                    nbits_increment=(10 * operand_value + 2) // 3,
                    scale_increment=operand_value,
                    refval_factor=10 ** operand_value,
                )

        elif operator_code == 208:  # change all string type descriptor length
            self.nbytes_new = operand_value

        # Data not present for following YYY descriptors except class 0-9 and 31
        elif operator_code == 221:
            self.data_not_present_count = operand_value

        # Quality info, substituted, 1st order stats, difference stats, replaced
        elif operator_code in (222, 223, 224, 225, 232):
            if operand_value == 0:
                self.bitmap_definition_state = BITMAP_INDICATOR
                self.add_process_worker_call('mark_back_reference_boundary')
                self.compile_process_0(descriptor)
                if operator_code == 222:
                    self.status_qa_info_follows = QA_INFO_WAITING
            else:  # 255 for markers (this does not apply to 222)
                self.compile_marker_operator_descriptor(descriptor)

        elif operator_code == 235:
            self.add_process_worker_call('cancel_all_back_references')

        elif operator_code == 236:
            self.compile_process_0(descriptor)

        elif operator_code == 237:
            if operand_value == 0:
                self.add_process_worker_call('recall_bitmap')

            else:  # 255 cancel re-used bitmap
                if self.most_recent_bitmap_is_for_reuse:
                    self.add_process_worker_call('cancel_bitmap')
            self.compile_process_0(descriptor)

        else:  # TODO: 241, 242, 243
            pass

    def compile_process_0(self, descriptor):
        """
        Many operator descriptors add a zero to the values.
        """
        # TODO: this is to be compatible to BUFRDC, necessary?
        self.add_process_worker_call('process_value_for_descriptor', 0, descriptor)

    def compile_marker_operator_descriptor(self, descriptor):
        # TODO: do we really need associated field for marker operators
        if self.nbits_associated_list:
            self.compile_associated_field(descriptor)

        self.add_process_worker_call(
            'process_bitmapped_descriptor',
            descriptor.id,
            self.nbytes_new,
            self.nbits_offset,
            self.scale_offset,
            self.nsr_modifier.nbits_increment,
            self.nsr_modifier.scale_increment,
            self.nsr_modifier.refval_factor)

    def compile_skipped_local_descriptor(self, descriptor):
        # TODO: possible associated field?
        self.add_process_worker_call(
            'process_codeflag',
            SkippedLocalDescriptor(descriptor.id, self.nbits_skipped_local_descriptor),
            self.nbits_skipped_local_descriptor
        )
        # reset it back to zero
        self.nbits_skipped_local_descriptor = 0

    def compile_bitmap_definition(self, member):

        if self.bitmap_definition_state == BITMAP_INDICATOR:
            # TODO: 236000 and 237000 are handled here. bad?
            if member.id == 236000:  # bitmap define for reuse
                self.most_recent_bitmap_is_for_reuse = True
                self.bitmap_definition_state = BITMAP_WAITING_FOR_BIT
                self.compile_init_bitmap_bit_count()

            elif member.id == 237000:  # re-call most recent definition
                self.bitmap_definition_state = BITMAP_NA

            else:  # direct bitmap definition (non-reuse)
                self.most_recent_bitmap_is_for_reuse = False
                self.bitmap_definition_state = BITMAP_WAITING_FOR_BIT
                self.compile_init_bitmap_bit_count()

        elif self.bitmap_definition_state == BITMAP_WAITING_FOR_BIT:
            if member.id == 31031:
                self.bitmap_definition_state = BITMAP_BIT_COUNTING
                self.compile_increase_bitmap_bit_count()

        elif self.bitmap_definition_state == BITMAP_BIT_COUNTING:
            if member.id == 31031:
                self.compile_increase_bitmap_bit_count()
            else:
                # TODO: for compressed data, ensure all bitmap is equal
                self.add_process_worker_call(
                    'define_bitmap',
                    ast.Name(id='n_031031', ctx=ast.Load()),
                    self.most_recent_bitmap_is_for_reuse)
                self.bitmap_definition_state = BITMAP_NA

    def compile_members(self, members):
        for member in members:
            member_type = type(member)

            # TODO: NOT using if-elif for following checks because they may co-exist???
            #      It is highly unlikely if not impossible

            # 221 YYY data not present for following YYY descriptors except class 0-9 and 31
            if self.data_not_present_count:
                self.data_not_present_count -= 1
                if member_type is ElementDescriptor:
                    X = member.X
                    if not (1 <= X <= 9 or X == 31):  # skipping
                        continue
                        # TODO: maybe the descriptor should still be kept and set its value to None?
                        #       So it helps to keep the structure intact??

            # Currently defining new reference values
            if self.nbits_new_refval:
                self.compile_define_new_refval(member)
                continue

            # 206 YYY signify data width for local descriptor
            if self.nbits_skipped_local_descriptor:
                self.compile_skipped_local_descriptor(member)
                continue

            # Currently defining new bitmap
            if self.bitmap_definition_state != BITMAP_NA:
                self.compile_bitmap_definition(member)

            # Now process normally
            if member_type is ElementDescriptor:
                self.compile_element_descriptor(member)

            elif member_type is FixedReplicationDescriptor:
                self.compile_fixed_replication_descriptor(member)

            elif member_type is DelayedReplicationDescriptor:
                self.compile_delayed_replication_descriptor(member)

            elif member_type is OperatorDescriptor:
                self.compile_operator_descriptor(member)

            elif member_type is SequenceDescriptor:
                self.compile_sequence_descriptor(member)

            else:
                raise BpclError('Cannot compile descriptor {} of type: {}'.format(
                    member.id, member_type))


def make_funcdef(stmt_list):
    """
    Make a function definition from the AST tree of compiled template. This
    function can be saved as a python module and imported to use. The function
    is responsible for set the correct process_xxx methods based on the
    arguments (is_decode, is_compressed) it receives.
    """
    func_names = {'n_subsets'}
    # Find all methods the worker function calls
    for tree in stmt_list:
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = node.func.id
                if func_name not in ('range', 'worker_func'):
                    func_names.add(node.func.id)

    body = [
        ast.ImportFrom(
            module='six.moves', names=[ast.alias(name='range', asname=None)], level=0
        ),
        # The prefix of the methods based on decode/encode
        ast.Assign(
            targets=[ast.Name(id='prefix', ctx=ast.Store())],
            value=ast.IfExp(test=ast.Name(id='is_decode', ctx=ast.Load()),
                            body=ast.Str(s='decode_'), orelse=ast.Str(s='encode_'))
        ),
        # The suffix of the methods based on compressed/uncompressed
        ast.Assign(
            targets=[ast.Name(id='suffix', ctx=ast.Store())],
            value=ast.IfExp(test=ast.Name(id='is_compressed', ctx=ast.Load()),
                            body=ast.Str(s='_compressed'), orelse=ast.Str(s='_uncompressed'))
        ),
    ]

    # assign all needed vm methods with the proper prefix and suffix
    for func_name in func_names:
        # All process_xxx methods need prefix and suffix
        if func_name.startswith('process_'):
            body.append(
                ast.Assign(
                    targets=[ast.Name(id=func_name, ctx=ast.Store())],
                    value=ast.Call(
                        func=ast.Name(id='getattr', ctx=ast.Load()),
                        args=[
                            ast.Name(id='vm', ctx=ast.Load()),
                            ast.Call(
                                func=ast.Attribute(value=ast.Str(s='{}{}{}'),
                                                   attr='format', ctx=ast.Load()),
                                args=[ast.Name(id='prefix', ctx=ast.Load()),
                                      ast.Str(s=func_name[8:]),
                                      ast.Name(id='suffix', ctx=ast.Load())],
                                keywords=[], starargs=None, kwargs=None)

                        ],
                        keywords=[], starargs=None, kwargs=None
                    )
                )
            )
        else:  # everything else just has a simple assignment
            body.append(
                ast.Assign(
                    targets=[ast.Name(id=func_name, ctx=ast.Store())],
                    value=ast.Attribute(value=ast.Name(id='vm', ctx=ast.Load()),
                                        attr=func_name, ctx=ast.Load())
                )
            )

    body.extend(stmt_list)

    if PY3:
        arguments = ast.arguments(
            args=[ast.arg(arg='vm', annotation=None),
                  ast.arg(arg='is_decode', annotation=None),
                  ast.arg(arg='is_compressed', annotation=None)],
            vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]
        )
        node = ast.FunctionDef(
            name='process_template',
            args=arguments,
            body=body,
            decorator_list=[],
            returns=None
        )
    else:
        arguments = ast.arguments(
            args=[ast.Name(id='vm', ctx=ast.Param()),
                  ast.Name(id='is_decode', ctx=ast.Param()),
                  ast.Name(id='is_compressed', ctx=ast.Param())],
            vararg=None, kwarg=None, defaults=[]
        )

        node = ast.FunctionDef(
            name='process_template',
            args=arguments,
            body=body,
            decorator_list=[]
        )

    return ast.Module(body=[node])


template_compiler = TemplateCompiler()
