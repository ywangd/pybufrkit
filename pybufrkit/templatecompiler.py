"""
pybufrkit.templatecompiler
~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import sys
import json
import logging
import contextlib

from pybufrkit.errors import PyBufrKitError
from pybufrkit.coder import Coder, CoderState
from pybufrkit.tables import TableGroupKey, TableGroupCacheManager
from pybufrkit.descriptors import Descriptor

__all__ = ['loads_compiled_template', 'TemplateCompiler', 'CompiledTemplateManager', 'process_compiled_template']

log = logging.getLogger(__file__)


# noinspection PyProtectedMember
def get_func_name():
    """
    Get name of the caller of this function. Helper to create the function call construct.
    """
    return sys._getframe(1).f_code.co_name


#############################################################################
# CompileTemplate and its components
class Statement(object):
    """
    A basic code construct.
    """

    def to_dict(self):
        return {'type': self.__class__.__name__}


class State031031Increment(Statement):
    """
    Add one to the 031031 count, n_031031 of the state object.
    """

    def __str__(self):
        return 'n_031031 + 1'


class State031031Reset(Statement):
    """
    Reset the 031031 count, n_031031 of the state object, to zero.
    """

    def __str__(self):
        return 'n_031031 = 0'


class MethodCall(Statement):
    """
    A generic method call
    """

    def __init__(self, method_name, args=(), state_properties=None):
        self.method_name = method_name
        self.args = args
        self.state_properties = state_properties

    def __str__(self):
        return '{}({})'.format(
            self.method_name, ','.join(str(x) for x in self.args)
        )

    def to_dict(self):
        d = super(MethodCall, self).to_dict()

        d.update({
            'method_name': self.method_name,
            'args': self.args,
            'state_properties': self.state_properties,
        })
        if len(self.args) > 0 and isinstance(self.args[0], Descriptor):
            d['args'] = (self.args[0].id,) + self.args[1:]
            d['with_descriptor'] = True
        else:
            d['args'] = self.args
            d['with_descriptor'] = False

        return d


class StateMethodCall(MethodCall):
    """
    A State object method call.
    """

    def __str__(self):
        return 'state.{}'.format(super(StateMethodCall, self).__str__())


class CoderMethodCall(MethodCall):
    """
    A Coder object method call.
    """

    def __str__(self):
        return 'coder.{}'.format(super(CoderMethodCall, self).__str__())


class Block(Statement):
    """
    A Block is a list of Statement
    """

    def __init__(self):
        self.statements = []

    def add_statement(self, statement):
        self.statements.append(statement)

    def __str__(self):
        return '[{}]'.format(','.join(str(x) for x in self.statements))

    def to_dict(self):
        d = super(Block, self).to_dict()
        d.update({
            'statements': [statement.to_dict() for statement in self.statements]
        })
        return d


class Loop(Block):
    """
    A loop construct has a loop variable and a block of statements to
    execute within the loop. The loop variable can be either a constant
    or a function call that returns the actual value for the loop counter.
    """

    def __init__(self, repeat):
        super(Loop, self).__init__()
        self.repeat = repeat

    def __str__(self):
        return '<{}, {}>'.format(self.repeat, super(Loop, self).__str__())

    def to_dict(self):
        d = super(Loop, self).to_dict()
        d.update({
            'repeat': self.repeat.to_dict() if issubclass(type(self.repeat), MethodCall) else self.repeat
        })
        return d


class CompiledTemplate(Block):
    """
    This class represents a compiled template.

    :param descriptors.BufrTemplate template: The template to compile
    """

    def __init__(self, table_group_key, template):
        super(CompiledTemplate, self).__init__()
        self.table_group_key = table_group_key
        self.template = template

    def to_dict(self):
        d = super(CompiledTemplate, self).to_dict()
        d.update({
            'table_group_key': self.table_group_key,
            'template_ids': self.template.original_descriptor_ids
        })
        return d


#############################################################################
# Template Compiler and helpers.
class CompilerState(CoderState):
    """
    A state class that is specifically for the TemplateCompiler. It helps
    to records all calls dispatched from the generic Coder and TemplateCompiler.
    """

    def __init__(self, table_group, template):
        super(CompilerState, self).__init__(False, 1)
        self.block_stack = [CompiledTemplate(table_group.key, template)]

    @property
    def compiled_template(self):
        assert len(self.block_stack) == 1, 'Code block stack must be 1 (was {})'.format(len(self.block_stack))
        return self.block_stack[0]

    @contextlib.contextmanager
    def new_loop(self, repeat):
        loop = Loop(repeat)
        self.block_stack[-1].add_statement(loop)
        self.block_stack.append(loop)
        yield loop
        self.block_stack.pop()

    def add_statement(self, statement):
        self.block_stack[-1].add_statement(statement)

    def mark_back_reference_boundary(self):
        self.add_statement(StateMethodCall(get_func_name()))

    def recall_bitmap(self):
        self.add_statement(StateMethodCall(get_func_name()))

    def cancel_bitmap(self):
        self.add_statement(StateMethodCall(get_func_name()))

    def cancel_all_back_references(self):
        self.add_statement(StateMethodCall(get_func_name()))

    def add_bitmap_link(self):
        self.add_statement(StateMethodCall(get_func_name()))


class TemplateCompiler(Coder):
    """
    The compiler for the BUFR Template. This class does its job by recording
    calls from the generic Coder.
    """

    def __init__(self):
        super(TemplateCompiler, self).__init__(None, None)

    def process(self, template, table_group):
        """
        Entry point of the Compiler.

        :param descriptors.BufrTemplate template: The BUFR template to compile
        :param tables.TableGroup table_group: The Table Group used to instantiate the Template.
        :return: CompiledTemplate
        """
        state = CompilerState(table_group, template)
        self.process_template(state, bit_operator=None, template=template)

        return state.compiled_template

    def process_section(self, bufr_message, bit_operator, section):
        pass

    def process_bitmap_definition(self, state, bit_operator, descriptor):
        n_031031 = state.n_031031
        super(TemplateCompiler, self).process_bitmap_definition(state, bit_operator, descriptor)
        if state.n_031031 == 0:
            state.add_statement(State031031Reset())
        elif state.n_031031 == n_031031 + 1:
            state.add_statement(State031031Increment())
        elif state.n_031031 == n_031031:
            pass
        else:
            raise PyBufrKitError('erroneous n_031031 change')

    def process_fixed_replication_descriptor(self, state, bit_operator, descriptor):
        with state.new_loop(descriptor.n_repeats):
            self.process_members(state, bit_operator, descriptor.members)

    def process_delayed_replication_descriptor(self, state, bit_operator, descriptor):
        # TODO: delayed repetition descriptor 031011, 031012
        if descriptor.id in (31011, 31012):
            raise NotImplementedError('delayed repetition descriptor')

        self.process_element_descriptor(state, bit_operator, descriptor.factor)
        with state.new_loop(CoderMethodCall('get_value_for_delayed_replication_factor')):
            self.process_members(state, bit_operator, descriptor.members)

    def process_bitmapped_descriptor(self, state, bit_operator, descriptor):
        state_properties = {
            'new_bytes': state.new_nbytes,
            'nbits_offset': state.nbits_offset,
            'scale_offset': state.scale_offset,
            'bsr_modifier': state.bsr_modifier,
        }
        state.add_statement(
            CoderMethodCall(get_func_name(), (descriptor,), state_properties=state_properties)
        )

    def get_value_for_delayed_replication_factor(self, state):
        pass

    def define_bitmap(self, state, reuse):
        state.add_statement(
            CoderMethodCall(get_func_name(), (reuse,))
        )

    def process_numeric(self, state, bit_operator, descriptor, nbits, scale_powered, refval):
        state.add_statement(
            CoderMethodCall(get_func_name(), (descriptor, nbits, scale_powered, refval))
        )

    def process_string(self, state, bit_operator, descriptor, nbytes):
        state.add_statement(CoderMethodCall(get_func_name(), (descriptor, nbytes)))

    def process_codeflag(self, state, bit_operator, descriptor, nbits):
        state.add_statement(CoderMethodCall(get_func_name(), (descriptor, nbits)))

    def process_new_refval(self, state, bit_operator, descriptor, nbits):
        state.new_refvals[descriptor.id] = None  # Actual value will be decided at runtime
        state.add_statement(CoderMethodCall(get_func_name(), (descriptor, nbits)))

    def process_numeric_of_new_refval(self, state, bit_operator,
                                      descriptor, nbits, scale_powered,
                                      refval_factor):
        state.add_statement(
            CoderMethodCall(get_func_name(), (descriptor, nbits, scale_powered, refval_factor))
        )

    def process_constant(self, state, bit_operator, descriptor, value):
        state.add_statement(CoderMethodCall(get_func_name(), (descriptor, value)))


# #############################################################################
# Functions to execute a compiled template.
def process_compiled_template(coder, state, bit_operator, compiled_template):
    """
    This function runs the compiled code from the TemplateCompiler

    :param Coder coder:
    :param VmState state:
    :param bit_operator:
    :param Block compiled_template:
    """
    process_statements(coder, state, bit_operator, compiled_template.statements)


def process_statements(coder, state, bit_operator, statements):
    """
    Process through a list of statements. Recursively call itself if the sub-statement
    is itself a list of statements.
    """
    for statement in statements:
        if isinstance(statement, MethodCall):
            # Populate any necessary state properties. This is to re-create the
            # modifier effects of operator descriptors.
            if statement.state_properties is not None:
                for k, v in statement.state_properties.items():
                    setattr(state, k, v)

            if type(statement) is StateMethodCall:
                getattr(state, statement.method_name)(*statement.args)

            elif type(statement) is CoderMethodCall:
                if issubclass(type(statement.args[0]), Descriptor):
                    getattr(coder, statement.method_name)(state, bit_operator, *statement.args)
                else:
                    getattr(coder, statement.method_name)(state, *statement.args)
            else:
                raise PyBufrKitError('Unknown statement: {}'.format(statement))

        elif isinstance(statement, State031031Reset):
            state.n_031031 = 0

        elif isinstance(statement, State031031Increment):
            state.n_031031 += 1

        elif type(statement) is Loop:
            if isinstance(statement.repeat, CoderMethodCall):
                repeat = getattr(coder, statement.repeat.method_name)(state)
            else:
                repeat = statement.repeat

            for _ in range(repeat):
                process_statements(coder, state, bit_operator, statement.statements)

        else:
            raise PyBufrKitError('Unknown statement: {}'.format(statement))


#############################################################################
# Management for compiled templates
class CompiledTemplateManager(object):
    """
    A management class for compiled templates that handles caching and lookup.

    :param cache_max: The maximum number of compiled templates to cache.
    """

    def __init__(self, cache_max):
        self.template_compiler = TemplateCompiler()
        self.cache_max = cache_max
        self.cache = {}

    def get_or_compile(self, template, table_group):
        """

        :param descriptors.BufrTemplate template: The BUFR template to compile
        :param tables.TableGroup table_group: The Table Group used to instantiate the Template.
        :return:
        """
        key_of_compiled_template = (
            tuple(template.original_descriptor_ids),
            table_group.key
        )
        log.debug('Getting compiled template of key: {}'.format(key_of_compiled_template))
        compiled_template = self.cache.get(key_of_compiled_template, None)

        if compiled_template is None:
            log.debug('Cached version not available. Compiling now ...')
            compiled_template = self.template_compiler.process(template, table_group)

            if self.cache_max > 0:
                # TODO: Better cache invalidate algorithm
                if len(self.cache) >= self.cache_max:
                    self.cache.popitem()

                self.cache[key_of_compiled_template] = compiled_template

        return compiled_template


#############################################################################
# Functions to Load a CompiledTemplate from a JSON String.
def loads_compiled_template(s):
    """
    Load a compiled template object from its JSON string representation.

    :param s: A JSON string represents the compiled template.
    :return: The compiled template
    """
    d = json.loads(s)
    assert d['type'] == 'CompiledTemplate', 'The type must be CompiledTemplate (was {})'.format(d['type'])
    # Table group key and its elements must be tuple to be hashable, which is a
    # requirement for being a key to dict.
    table_group = TableGroupCacheManager.get_table_group_by_key(
        TableGroupKey(*[(tuple(x) if isinstance(x, list) else x) for x in d['table_group_key']])
    )
    template = table_group.template_from_ids(*d['template_ids'])
    compiled_template = CompiledTemplate(table_group.key, template)
    for statement_dict in d['statements']:
        compiled_template.add_statement(
            STATEMENT_LOAD_FUNCS[statement_dict['type']](table_group, statement_dict)
        )
    return compiled_template


def load_loop_from_dict(table_group, d):
    assert d['type'] == 'Loop', 'The type must be Loop (was {})'.format(d['type'])
    if isinstance(d['repeat'], dict):
        repeat = STATEMENT_LOAD_FUNCS[d['repeat']['type']](table_group, d['repeat'])
    else:
        repeat = d['repeat']

    loop = Loop(repeat)
    for statement_dict in d['statements']:
        loop.add_statement(
            STATEMENT_LOAD_FUNCS[statement_dict['type']](table_group, statement_dict)
        )
    return loop


def load_coder_method_call_from_dict(table_group, d):
    assert d['type'] == 'CoderMethodCall', 'The type must be CoderMethodCall (was {})'.format(d['type'])
    return load_method_call_from_dict(CoderMethodCall, table_group, d)


def load_state_method_call_from_dict(table_group, d):
    assert d['type'] == 'StateMethodCall', 'The type must be StateMethodCall (was {})'.format(d['type'])
    return load_method_call_from_dict(StateMethodCall, table_group, d)


def load_method_call_from_dict(method_type, table_group, d):
    if d.get('with_descriptor', False):
        descriptor = table_group.lookup(d['args'][0])
        args = tuple([descriptor] + d['args'][1:])
    else:
        args = tuple(d['args'])

    return method_type(method_name=d['method_name'],
                       args=args,
                       state_properties=d.get('state_properties'))


STATEMENT_LOAD_FUNCS = {
    'Loop': load_loop_from_dict,
    'CoderMethodCall': load_coder_method_call_from_dict,
    'StateMethodCall': load_state_method_call_from_dict,
    'State031031Increment': lambda *args: State031031Increment(),
    'State031031Reset': lambda *args: State031031Reset(),
}
